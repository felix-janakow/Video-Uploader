"""
Video Uploader (IBM Aspera -> IBM Cloud Object Storage)

This script scans a directory for video files (.mp4/.mov), builds an
Aspera TransferSpecV2 for IBM Cloud Object Storage (COS), starts the
transfer via the Aspera Transfer Manager daemon (transferd), and monitors
progress until completion.

Key features:
- Loads credentials from a .env file (no secrets in code)
- Accepts a directory (or a single file) and discovers .mp4/.mov files
- Builds a JSON transfer spec in-line (per SDK examples)
- Preserves original filenames by default
- Optional dry-run prints the transfer spec without uploading

Environment variables expected (see .env):
- IBMCLOUD_API_KEY
- IBMCLOUD_BUCKET
- IBMCLOUD_COS_INSTANCE_ID
- IBMCLOUD_COS_ENDPOINT
- Optional: ASPERA_REMOTE_HOST (default ats-sl-fra.aspera.io --> Frankfurt)
- Optional: COS_DESTINATION (default /aspera-uploads)

CLI (short):
- python main.py [directory] [--dry-run] [--transfer-manager-host HOST:PORT] [--no-folder-marker]

Prerequisites:

- The IBM Aspera Transfer SDK has to be dowloaded manually from: 
    https://developer.ibm.com/apis/catalog/aspera--aspera-transfer-sdk/downloads/downloads.json

- The SDK has to be built with the following command:
    mkdir -p transferd_api
    echo " 4 Python-Bindings generieren..."
    python3 -m grpc_tools.protoc -I.. \
            --python_out=transferd_api \
        --grpc_python_out=transferd_api \
    pip install .

- To run the script, installing grpcio, grpcio-tools, and protobuf within your environmet are required for the SDK
- The transferd daemon has to be running on the same machine as this script
    --> You can find the daemon within the sbin directory of the SDK
    --> Start the daemon with the following command (after navigating to the SDK directory):
            ./transferd --address 127.0.0.1 --port 55002


Note: This file could potentially be changed to upload other types of files other than .mp4/.mov
        --> This could be done by changing the find_videos() function to accept a list of other file extensions

"""

# ----------------------------------------------------------------------------
# Library and third-party imports
# ----------------------------------------------------------------------------
from dotenv import load_dotenv
import argparse
import os
import sys
from pathlib import Path
import json
import grpc
import time

# ----------------------------------------------------------------------------
# Aspera SDK Python stubs setup
# ----------------------------------------------------------------------------
SDK_PY_PATH = Path(__file__).parent / "ibm-aspera-transfer-sdk-macos-arm64-1.1.6" / "api" / "python"
if str(SDK_PY_PATH) not in sys.path:
    # We prepend (index 0) to ensure our local stubs take precedence over any
    sys.path.insert(0, str(SDK_PY_PATH))

# gRPC stubs from the SDK
try:
    import transferd_api.transferd_pb2 as transfer_manager
    import transferd_api.transferd_pb2_grpc as transfer_manager_grpc
    from transferd_api.transferd_pb2 import TransferRequest, TransferConfig, RegistrationRequest, RegistrationFilter
    
except ImportError as e:
    print("Could not import Aspera SDK Python stubs. Make sure the SDK path is correct and gRPC is installed.")
    print(f"Import error: {e}")
    # ⚠️ Ensure packages 'grpcio' and 'protobuf' are installed in the active venv.
    # ⚠️ Verify the SDK path above is correct for your environment.
    sys.exit(1)

# ----------------------------------------------------------------------------
# Environment loading (.env)
# ----------------------------------------------------------------------------
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)


class VideoUploader:
    """Encapsulates discovery, spec construction, connection, and monitoring."""

    def __init__(self, transfer_manager_host="localhost:55002", create_dir=True):
        self.transfer_manager = transfer_manager_host
        self.client = None
        self.create_dir = create_dir

        # Environment Variables
        # These are read once on startup. If you change .env values, restart the script.
        self.api_key = os.environ.get("IBMCLOUD_API_KEY")
        self.bucket = os.environ.get("IBMCLOUD_BUCKET")
        self.service_instance_id = os.environ.get("IBMCLOUD_COS_INSTANCE_ID")
        self.service_endpoint = os.environ.get("IBMCLOUD_COS_ENDPOINT")
        self.remote_host = os.environ.get("ASPERA_REMOTE_HOST", "ats-sl-fra.aspera.io")
        self.destination = os.environ.get("COS_DESTINATION", "/aspera-uploads")

    def _validate_environment(self):
        """Ensure required environment variables are present for real uploads."""
        required_vars = [
            ("IBMCLOUD_API_KEY", self.api_key),
            ("IBMCLOUD_BUCKET", self.bucket),
            ("IBMCLOUD_COS_INSTANCE_ID", self.service_instance_id),
            ("IBMCLOUD_COS_ENDPOINT", self.service_endpoint),
        ]
        missing = [name for name, val in required_vars if not val]
        if missing:
            print("Missing environment variables:", missing)
            sys.exit(1)
        print("✅ Environment loaded successfully")

    def find_video_files(self, path_like):
        """Return a list of absolute video file paths under a directory or for a single file.

        Accepts a path to either a directory (scanned recursively) or a single
        file. Filters by common video extensions (.mp4/.mov).
        """
        p = Path(path_like)
        video_types = [".mp4", ".mov", ".MP4", ".MOV"]
        # If you`d like to add other extensions, extend this list.
        if p.is_file():
            # If a single file was provided, accept it directly
            return [str(p.resolve())] if p.suffix in video_types else []
        # Else, treat as directory and scan recursively
        # rglob('*') walks every subfolders 
        directory = p
        return [str(f.resolve()) for f in directory.rglob("*") if f.suffix in video_types and f.is_file()]

    def create_transfer_spec(self, file_paths):
        """Build and return a TransferSpecV2-compatible Python dict.

        Notes
        -----
        - session_initiation.icos: COS credentials and endpoint
        - file_system.create_dir: whether to auto-create destination prefix
        - assets.destination_root: COS key prefix (treated as a directory)
        - assets.paths: list of {"source": "/abs/local/path"}
        """
        # Build a TransferSpecV2-compatible JSON dict per SDK examples
        # Minimal inline endpoint fix: ensure https:// prefix if missing
        endpoint = self.service_endpoint or ""
        if endpoint and not (endpoint.startswith("http://") or endpoint.startswith("https://")):
            # The transfer daemon expects a full URL; default to https if no scheme is provided.
            endpoint = f"https://{endpoint}"

        # Minimal inline destination fix: ensure it's treated as a directory/prefix
        # We normalize to start with '/' and end with '/' to express a folder-like prefix
        destination_root = (self.destination or "/").strip()
        if not destination_root.startswith("/"):
            destination_root = "/" + destination_root
        if not destination_root.endswith("/"):
            destination_root = destination_root + "/"
        # With a trailing slash, most UIs present this as a "folder" prefix.
        # See also the --no-folder-marker flag which controls create_dir behavior.

        # Keep original filenames by default: only provide source paths
        path_entries = [{"source": f} for f in file_paths]
        # By providing only "source", Aspera preserves the original filename at the destination.
        # You can override per-file names by adding a "destination" key per entry.

        return {
            "session_initiation": {
                "icos": {
                    "api_key": self.api_key,
                    "bucket": self.bucket,
                    "ibm_service_instance_id": self.service_instance_id,
                    "ibm_service_endpoint": endpoint,
                }
            },
            # Ensure destination prefix is created if it doesn't exist
            "file_system": {
                "create_dir": bool(self.create_dir)
                # True (default): transferd can create the destination prefix (some UIs show a 0-byte marker)
                # False (with --no-folder-marker): do not create; ensure the prefix exists or use '/'
            },
            "direction": "send",
            "remote_host": self.remote_host,  # Aspera host = default ATS shared service
            "title": "video file upload",
            "assets": {
                "destination_root": destination_root,  # COS key prefix ("folder" where files land)
                "paths": path_entries,
            },
        }

    def show_transfer_spec(self, file_paths):
        """Pretty-print the generated transfer spec (for --dry-run)."""
        spec = self.create_transfer_spec(file_paths)
        print("ℹ️ Transfer specification:")
        print(json.dumps(spec, indent=2))

    def connect(self):
        """Establish a gRPC client to the Aspera Transfer Manager daemon."""
        if not self.client:
            try:
                self.client = transfer_manager_grpc.TransferServiceStub(
                    grpc.insecure_channel(self.transfer_manager)
                )
                print("✅ Connected to Transfer Manager")
            except Exception as e:
                print(f"❌ Could not connect: {e}")
                sys.exit(1)

    def get_status_text(self, status_code):
        """Map SDK status code to a human-readable form."""
        status_map = {
            transfer_manager.QUEUED: "Queued",
            transfer_manager.RUNNING: "Running",
            transfer_manager.COMPLETED: "Completed",
            transfer_manager.FAILED: "Failed",
            transfer_manager.PAUSED: "Paused"
        }
        return status_map.get(status_code, f"Unknown ({status_code})")

    def upload_videos(self, file_paths, dry_run=False):
        """Validate inputs, optionally --dry-run, then start and monitor a transfer."""
        if not file_paths:
            print("❌ No video files found")
            return

        # Normalize to absolute paths and validate
        abs_files = []
        for f in file_paths:
            p = Path(f).resolve()
            if not p.exists():
                print(f"❌ Source not found: {p}")
                continue
            if not p.is_file():
                print(f"❌ Not a file: {p}")
                continue
            if not os.access(p, os.R_OK):
                print(f"❌ Not readable: {p}")
                continue
            abs_files.append(str(p))

        if not abs_files:
            print("❌ No valid, readable source files to upload")
            return

        print(f"📹 Found {len(abs_files)} video(s) to upload:")
        total_size = 0
        for f in abs_files:
            try:
                size_mb = os.path.getsize(f) / (1024 * 1024)
                print(f"  - {f} ({size_mb:.1f} MB)")
                total_size += size_mb
            except OSError:
                print(f"  - {f}")
        
        print(f"Total size: {total_size:.1f} MB")

        transfer_spec = self.create_transfer_spec(abs_files)

        if dry_run:
            print("\n--- DRY RUN ---")
            print(json.dumps(transfer_spec, indent=2))
            return

        # Create gRPC client if not already connected
        # We use insecure_channel for localhost. If transferd is remote and TLS-enabled,
        # you'd swap this for a secure channel configuration in connect().
        self.connect()

        # Validate environment only for actual transfers
        self._validate_environment()  # Only enforce when actually uploading

        # Package the JSON transfer spec into the gRPC TransferRequest
        transfer_request = TransferRequest(
            transferType=transfer_manager.FILE_REGULAR,
            config=TransferConfig(),  # Default transfer config; tune as needed
            transferSpec=json.dumps(transfer_spec),
        )

        try:
            print("\n🚀 Starting transfer...")
            response = self.client.StartTransfer(transfer_request)
            transfer_id = response.transferId
            print(f"✅ Transfer started with ID: {transfer_id}")

            # Monitor Transfer: subscribe only to this transferId
            registration_request = RegistrationRequest()
            registration_filter = RegistrationFilter()
            registration_filter.transferId.append(transfer_id)
            registration_request.filters.append(registration_filter)

            start_time = time.time()
            timeout_seconds = 300

            for info in self.client.MonitorTransfers(registration_request):
                t_id = getattr(info, "transferId", "unknown")
                status_code = getattr(info, "status", -1)
                # TransferResponse carries an embedded TransferInfo with bytesTransferred
                try:
                    progress_bytes = info.transferInfo.bytesTransferred if info.HasField("transferInfo") else 0
                except Exception:
                    progress_bytes = 0
                progress_mb = progress_bytes / (1024 * 1024)
                elapsed = int(time.time() - start_time)
                print(f"[{elapsed}s] Transfer {t_id}: {self.get_status_text(status_code)} - {progress_mb:.1f} MB of {total_size:.1f} MB")

                if status_code in (transfer_manager.COMPLETED, transfer_manager.FAILED):
                    # Terminal states: break the monitor loop
                    if status_code == transfer_manager.COMPLETED:
                        print("✅ Transfer completed successfully!")
                    else:
                        print("❌ Transfer failed!")
                    break

                if elapsed > timeout_seconds:
                    # Safety valve: do not monitor forever. Transfer may still continue server-side.
                    print(f"⏰ Timeout ({timeout_seconds}s). Transfer may still be running.")
                    break

        except Exception as e:
            print(f"❌ Transfer error: {e}")
            msg = str(e)
            if "Destination path is not a directory" in msg:
                print("➡️ Hint: Set COS_DESTINATION to a directory prefix (e.g. '/', '/Upload/', or '/my-prefix/').")
                print("    The destination must end with a trailing slash to be treated as a folder/prefix.")
            # For other errors, consult transferd logs and verify endpoint, bucket, and permissions.
            sys.exit(1)


def main():
    """Parse CLI args, discover videos, and perform dry-run or upload."""
    parser = argparse.ArgumentParser(
        description="Upload video files to IBM COS using Aspera"
    )
    parser.add_argument('directory', nargs='?', default='.', help="Directory to scan")
    parser.add_argument('--dry-run', action="store_true", help="Show transfer spec without uploading")
    parser.add_argument('--transfer-manager-host', default='localhost:55002', help="Transfer Manager host:port")
    parser.add_argument('--no-folder-marker', action='store_true', help="Do not create destination folder marker (sets file_system.create_dir=false)")
    # Use --no-folder-marker if your bucket UI shows an extra 0-byte object named like your folder.
    args = parser.parse_args()

    uploader = VideoUploader(transfer_manager_host=args.transfer_manager_host, create_dir=not args.no_folder_marker)
    videos = uploader.find_video_files(args.directory)

    if not videos:
        print("❌ No videos found")
        sys.exit(1)

    uploader.upload_videos(videos, dry_run=args.dry_run)

    if args.dry_run:
        print("ℹ️ Dry run complete")


if __name__ == "__main__":
    main()
