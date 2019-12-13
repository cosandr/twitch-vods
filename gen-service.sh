#!/bin/bash

# Run python in shell to avoid SELinux errors

if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root" 
   exit 1
fi

_user="andrei"
_group="users"
_path_exec="/srv/containers/twitch/src"
_path_rec="/tank/media/twitch_raw"

_install_path_rec="/etc/systemd/system/rl-rec.service"

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

_install_path_enc="/etc/systemd/system/rl-enc.service"
_path_enc="/tank/media/twitch"

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

_install_path_www="/etc/systemd/system/rl-www.service"
_path_www="/var/www/main/clips"
_path_clips="/tank/media/clips"

cat > $_install_path_www <<EOF
[Unit]
Description=Generates symlinks to clips and RL vods
After=postgresql-12.service
After=zfs-mount.service
Requires=postgresql-12.service
ConditionPathExists=$_path_enc/RichardLewisReports
ConditionPathExists=$_path_clips

[Service]
Type=simple
Restart=on-failure
WorkingDirectory=$_path_exec
User=$_user
Group=$_group
Environment=PATH_WWW=$_path_www
Environment=PATH_SRC1=$_path_clips
Environment=PATH_SRC2=$_path_enc/RichardLewisReports
Environment=PATH=/opt/pyenv/versions/twitch/bin:/usr/bin:\$PATH
ExecStart=bash -c "python uuid-gen.py"

[Install]
WantedBy=multi-user.target
EOF

chmod 644 $_install_path_rec
chmod 644 $_install_path_enc
chmod 644 $_install_path_www
systemctl daemon-reload
cat $_install_path_rec
cat $_install_path_enc
cat $_install_path_www
