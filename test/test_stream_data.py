import json
from datetime import timezone, datetime

from dateutil.parser import isoparse

from modules.recorder import StreamData


def test_stream_data_sample():
    sample_file = 'test/samples/live_kraken.json'
    with open(sample_file, 'r') as fr:
        data = json.load(fr)
    actual = StreamData.from_json(data)
    expected = data['stream']
    expected_time = isoparse(expected['created_at'])
    assert actual.type == expected['stream_type']
    assert actual.created_at.astimezone(timezone.utc) == expected_time
    assert actual.preview == expected['preview']['medium']
    assert actual.user_logo == expected['channel']['logo']
    assert actual.title == expected['channel']['status']
    assert actual.url == expected['channel']['url']


def test_stream_data_manual():
    expected = dict(
        type_='live',
        created_at=datetime(2020, 1, 2),
        preview="https://example.com/preview.png",
        user_logo="https://example.com/user_logo.png",
        title="Stream title",
        url="https://www.twitch.tv/user",
    )
    actual = StreamData(**expected)
    assert actual.type == expected['type_']
    assert actual.created_at == expected['created_at']
    assert actual.preview == expected['preview']
    assert actual.user_logo == expected['user_logo']
    assert actual.title == expected['title']
    assert actual.url == expected['url']
