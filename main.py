import argparse             # For command line arguments
import json     
import os
import glob
import sys
from pathlib import Path    # Better file path handling
import grpc                 # For talking to transfer manager

sdk_path = Path(__file__).parent / "ibm-aspera-transfer-sdk-macos-arm64-1.1.6"/"api"/"python"/"transferd_api"
sys.path.insert(0, str(sdk_path))

try: 
    import transferd_api.transferd_pb2 as transfer_manager
    import transferd_api.transferd_pb2_grpc as transfer_manager_grpc
    #print("✅ Import successfull")

except ImportError:
    print("Protobuf installation needed to run this code")
    #sys.exit(2)

class VideoUploader:
    def __init__ (self, transfer_manager_host="localhost:55002"):
    
        self.transfer_manager = transfer_manager_host
        self.client = None

        self.api_key = os.environ.get("IBMCLOUD_API_KEY")
        self.bucket = os.environ.get("IBMCLOUD_BUCKET")
        self.service_instance_id = os.environ.get("IBMCLOUD_COS_INSTANCE_ID")
        self.service_endpoint = os.environ.get("IBMCLOUD_COS_ENDPOINT")

        self.remote_host = os.environ.get("ASPERA_REMOTE_HOST", "https://ats-sl-fra.aspera.io:443")
        self.destination = os.environ.get("COS_DESTINATION", "/aspera-uploads")
        self._validatate_environment()

    def _validatate_environment(self):
        required_vars = [
            ("IBMCLOUD_API_KEY", self.api_key), 
            ("IBMCLOUD_BUCKET", self.bucket), 
            ("IBMCLOUD_COS_INSTANCE_ID", self.service_instance_id),
            ("IBMCLOUD_COS_ENDPOINT", self.service_endpoint),
        ]
        missing_vars = [name for name, val in required_vars if val is None]
        if missing_vars:
            print("Missing required environment variables:")
            for var in missing_vars:
                print(f"Set {var} before running again")
        else: 
            print("✅ Confiuration loaded successfully")

def find_video_files(self, directory):
    video_types = [".mp4", ".mov", ".MP4", ".MOV"]
    video_files = []

    directory = Path(directory)
    print("Looking for video files in", directory)

    for video in directory.rglob("*"):
        if video.suffix in video_types and video.is_file():
            video_files.append(str(video))
        
    return video_files

def create_transfer_spec(self, filepaths):
    paths = []

    for filepath in filepaths:
        paths.append({"source": filepath})

    trasfer_spec = {
        "session_initialization": {
            "icons": {
                "api_key": self.api_key,
                "bucket": self.bucket,
                "ibm_service_instance_id": self.service_instance_id,
                "ibm_service_endpoint": self.service_endpoint
            }
        },

        "direction": "send",
        "remote_host": self.remote_host,
        "title": "video file upload",

        "assets": {
            "destination_root": self.destination,
            "paths": paths,
        },
    }

    
    return json.dumps(trasfer_spec, indent=2)


def show_transfer_spec(self, filepaths):
    spec = create_transfer_spec(self, filepaths)
    print("ℹ️ Transfer specification created: ")
    print(spec)

def  connect(self):
    try:
        self.client = transfer_manager_grpc.TransferServiceStub(grpc.insecure_channel(self.transfer_manager))
        print("✅ Connected to transfer manager")
    except Exception as e:
        print(f"❌ Could not connect to transfer manager: {e}")
        sys.exit(1)


def upload_videos(self, file_path, dry_run=False):
    print(f"Total of {len(file_path)} video files to upload")
    total_size = 0
    for file in file_path:
        try:
            fileSize = os.path.getsize(file) / (1024 * 1024) # in MB
            total_size += fileSize
            print(f" - {os.path.basename(file)}: {fileSize:.1f} MB")
        except OSError:
            print(f"❌ Could not get file size for{file}")

    print(f"Total size: {total_size:.1f} MB")
    print(f"Destination: {self.bucket}{self.destination}")
        


def main():

    parser = argparse.ArgumentParser(
        description = "Upload video files to IBM COS using Aspera",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
        Examples: 
        python main.py                              # Upload videos from current
        directory
        python main.py /path/to/other/video         # Upload from specific directory
        python main.py --dry-run                    # Show what would be uploade (without really uploading)
        '''
    )

    parser.add_argument(
        'directory',
        nargs='?',        # optional
        default='.',      # default to current directory
        help="Directory to scan for video files (default: current directory)"  
    )

    parser.add_argument(
        '--dry-run',
        action="store_true",
        help='Show what would be uploaded without actually transferring'
    )

    parser.add_argument(
        '--transfer-manager-host',
        default='localhost:55002',
        help='Transfer Manager daemon host:port'
    )

    args = parser.parse_args()

    
    print("🔄 Starting Video Uploader ...")
    
    uploader = VideoUploader()
    
    video_files_finder_execute = find_video_files(uploader, directory=Path(__file__).parent)

    for file in video_files_finder_execute:
        print(" 📹 Found Video:", file)

    if not video_files_finder_execute:
        print("❌ Error: Directory is empty or corrupt")
        sys.exit(1)
    
    if args.dry_run:
        print("DRY RUN MODE - No files will be uploaded")
        show_transfer_spec(uploader, video_files_finder_execute)

    else: 
        print("🚀 Starting transfer with IBM ASPERA...")
        show_transfer_spec(uploader, video_files_finder_execute)
    
    
    
             
if __name__ == "__main__":
    main()

        

        