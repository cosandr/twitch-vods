import pytest
import asyncio
import os
from discord import Embed

from modules.notifier import Notifier

LOOP = asyncio.get_event_loop()
NOTIFIER = Notifier(LOOP, webhook_url=os.environ['WEBHOOK_URL'])

@pytest.mark.skip
def test_send_notification():
    LOOP.run_until_complete(NOTIFIER.send('notifier test'))


def test_send_embed():
    embed = Embed()
    embed.title = 'RichardLewisReports'
    embed.description = 'Live stream recording started: Return Of By The Numbers #120'
    embed.set_thumbnail(url='https://static-cdn.jtvnw.net/jtv_user_pictures/1975b18f-fa7d-443f-b191-fba08f92f3a2-profile_image-300x300.jpeg')
    embed.set_image(url='https://static-cdn.jtvnw.net/previews-ttv/live_user_esl_csgo-320x180.jpg')
    icon = 'https://www.dresrv.com/icons/twitch-recorder.png'
    embed.set_author(name='Twitch Recorder', icon_url=icon)
    LOOP.run_until_complete(NOTIFIER.send(embed=embed))
