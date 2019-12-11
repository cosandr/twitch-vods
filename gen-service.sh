#!/bin/bash

# Run python in shell to avoid SELinux errors

if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root" 
   exit 1
fi

_install_path="/etc/systemd/system/rl-rec.service"
_user="andrei"
_group="users"
_path_exec="/home/andrei/git-projects/rl-rec-systemd"
_path_rec="/tank/media/twitch"
_path_raw="/tank/media/twitch_raw"
_path_busy="/srv/docker/.busy"
_path_py="/opt/pyenv/versions/twitch/bin/python"

cat > $_install_path <<EOF
[Unit]
Description=Records all RL twitch streams
After=zfs-mount.service
ConditionPathExists=$_path_rec
ConditionPathExists=$_path_raw
ConditionPathExists=$_path_busy

[Service]
Type=simple
Restart=on-failure
WorkingDirectory=$_path_exec
User=$_user
Group=$_group
Environment=STREAM_USER=RichardLewisReports
Environment=CHECK_TIMEOUT=120
Environment=PATH_RAW=$_path_raw
Environment=PATH_PROC=$_path_rec
Environment=PATH_BUSY=$_path_busy
ExecStart=sh -c "$_path_py twitch_async.py"

[Install]
WantedBy=multi-user.target
EOF

chmod 644 $_install_path
systemctl daemon-reload
cat $_install_path
