#!/bin/bash

# Run python in shell to avoid SELinux errors

if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root" 
   exit 1
fi

_install_path_rec="/etc/systemd/system/rl-rec.service"
_install_path_enc="/etc/systemd/system/rl-enc.service"
_user="andrei"
_group="users"
_path_exec="/home/andrei/git-projects/rl-vods"
_path_rec="/tank/media/twitch_raw"
_path_enc="/tank/media/twitch"

cat > $_install_path_rec <<EOF
[Unit]
Description=Records all RL twitch streams
After=zfs-mount.service
After=network.target
ConditionPathExists=$_path_rec

[Service]
Type=simple
Restart=on-failure
WorkingDirectory=$_path_exec
User=$_user
Group=$_group
Environment=STREAM_USER=RichardLewisReports
Environment=CHECK_TIMEOUT=120
Environment=PATH_RAW=$_path_rec
Environment=PATH=/opt/pyenv/versions/twitch/bin:/usr/bin:\$PATH
ExecStart=sh -c "python rec.py"

[Install]
WantedBy=multi-user.target
EOF

cat > $_install_path_enc <<EOF
[Unit]
Description=Encode Twitch stream recordings
After=zfs-mount.service
ConditionPathExists=$_path_enc

[Service]
Type=simple
Restart=on-failure
WorkingDirectory=$_path_exec
User=$_user
Group=$_group
Environment=PATH_PROC=$_path_enc
Environment=PATH=/opt/pyenv/versions/twitch/bin:/usr/bin:\$PATH
ExecStart=sh -c "python encode.py"

[Install]
WantedBy=multi-user.target
EOF

chmod 644 $_install_path_rec
chmod 644 $_install_path_enc
systemctl daemon-reload
cat $_install_path_rec
cat $_install_path_enc
