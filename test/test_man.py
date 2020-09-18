import os

from modules import IntroTrimmer

job = {
  "input": "200917-2259_richardlewisreports_Return_Of_By_The_Numbers_130.flv",
  "title": "Return Of By The Numbers #130",
  "user": "RichardLewisReports",
  "created_at": "2020-09-17T22:59:29+02:00"
}
cfg_path = 'data/trimmer/config.json'
src_path = '/dresrv/tank/downloads/twitch'


t = IntroTrimmer(cfg_path)
in_fp = os.path.join(src_path, job['input'])
sec = t.find_intro(in_fp, check_name=job['title'])
print(sec)
