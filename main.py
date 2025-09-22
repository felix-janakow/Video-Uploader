import argparse             # For command line arguments
import json     
import os
import glob
import sys
from pathlib import Path    # Better file path handling
import gprc                 # For talking to transfer manager

try: 
    import transfer_pb2 as transfer_manager
    import transfer