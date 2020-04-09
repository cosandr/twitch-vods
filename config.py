import os
import platform


# Time format
TIME_FMT = '%y%m%d-%H%M'
# Set to None for auto-detect, socket on Unix, TCP on Windows
TCP = None
TCP_PORT = 3626
JSON_SERIALIZE = None
SOCKET_FILE = 'encode.sock'
if TCP is None:
    if platform.system() == 'Windows' or os.getenv('USE_TCP', 'false').lower() == 'true':
        TCP = True
    else:
        TCP = False
if JSON_SERIALIZE is None:
    if os.getenv('JSON_SERIALIZE', 'false').lower() == 'true':
        JSON_SERIALIZE = True
    else:
        JSON_SERIALIZE = False
