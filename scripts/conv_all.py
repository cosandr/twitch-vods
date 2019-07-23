import subprocess
import os

for file in os.listdir("../raw"):
    tmp = file.split("_")
    if len(tmp) < 3:
        continue
    rectime = tmp[1] + "_" + tmp[2]
    name = " ".join(tmp[3:-1])
    mp4name = F"{name}_{rectime}.mp4"
    proc_path = F"/mnt/vods/{tmp[0]}"
    full_path = F"{proc_path}/{mp4name}"
    source = F"../raw/{file}"
    print(F"Converting {file} to {full_path}")
    subprocess.run(['ffmpeg', '-i', source, '-err_detect', 'ignore_err',
                    '-f', 'mp4', '-acodec', 'aac', '-c', 'copy', '-y',
                    '-v', 'info', '-hide_banner', full_path],
                    check=True)