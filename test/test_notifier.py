import pytest
import asyncio
from discord import Embed

from modules.notifier import Notifier

LOOP = asyncio.get_event_loop()
NOTIFIER = Notifier(LOOP)

@pytest.mark.skip
def test_send_notification():
    LOOP.run_until_complete(NOTIFIER.send('notifier test'))


def test_send_embed():
    embed = Embed()
    embed.title = 'RichardLewisReports'
    embed.description = 'Live stream recording started: Return Of By The Numbers #120'
    embed.set_thumbnail(url='https://static-cdn.jtvnw.net/jtv_user_pictures/richardlewisreports-profile_image-3b5eb60f8f2a79d0-300x300.jpeg')
    embed.set_image(url='https://static-cdn.jtvnw.net/previews-ttv/live_user_esl_csgo-{width}x{height}.jpg'.format(width=320, height=180))
    icon = 'https://www.dresrv.com/icons/twitch-recorder.png'
    embed.set_author(name='Twitch Recorder', icon_url=icon)
    LOOP.run_until_complete(NOTIFIER.send(embed=embed))
