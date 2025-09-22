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
    print("Import successfull ✅")

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
        print("Confiuration loaded successfully ✅")

    def _validatate_environment(self):
        required_vars = [
            ("IBMCLOUD_API_KEY", self.api_key), 
            ("IBMCLOUD_BUCKET", self.bucket), 
            ("IBMCLOUD_COS_INSTANCE_ID", self.service_instance_id),
            ("IBMCLOUD_COS_ENDPOINT", self.service_endpoint),
        ]
        missing_vars = [name for name, val in required_vars if val is None]
        if missing_vars:
            print("Missing required environment variables")
            for var in missing_vars:
                print(f"Set {var} before running again")


def main():
    print("Starting Video Uploader ... 🔄")
    uploader = VideoUploader()
    
             
if __name__ == "__main__":
    main()

        

        