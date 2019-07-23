import os

path = '../vods/RichardLewisReports'
for file in os.listdir(path):
    oldname = F"{path}/{file}"
    # if (file.startswith("RichardLewisReports_")):
    #     newname = file.strip("RichardLewisReports_")
    #     tmp = newname.split('_')
    #     date = tmp[0].split('-')
    #     newdate = F"{date[2]}-{date[1]}-{date[0]}"
    #     time = tmp[1].replace('.', '-')
    #     new_datetime = F"{time}_{newdate}"
    #     newfile = ' '.join(tmp[2:]).strip('.mp4')
    #     newname = F"{newfile}_{new_datetime}.mp4"
    #     print(F"{file} -> {newname}")
    #     os.rename(file, newname)
    # 18-10-18_1303
    if (file.endswith(".mp4")):
        tmp_split = file.split('_')
        time = tmp_split[1].split('-')
        newtime = ''.join(time)
        date = tmp_split[2].strip('.mp4')
        date_split = date.split('-')
        newdate = F"{date_split[0]}-{date_split[1]}-{date_split[2][2:]}"
        new_datetime = F"{newdate}_{newtime}"
        newname = F"{tmp_split[0]}_{new_datetime}.mp4"
        newname_pathed = F"{path}/{newname}"
        print(F"{file} -> {newname}")
        os.rename(oldname, newname_pathed)
