import os
import re

path = '/mnt/vods/RichardLewisReports'
for file in os.listdir(path):
    oldname = F"{path}/{file}"
    if file.endswith(".mp4"):
        newname = re.sub(r'[<>:"\/\\|?*\n]+', '', file)
        if newname == file:
            continue
        newname_pathed = os.path.join(path, newname)
        print(F"{file} -> {newname}")
        os.rename(oldname, newname_pathed)
