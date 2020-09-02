import asyncio
import pytest
from modules import Recorder

ESL_CSGO_ID = 31239503


class TestTwitchAPI:
    @classmethod
    def setup_class(cls):
        cls.loop = asyncio.get_event_loop()
        cls.rec = Recorder(cls.loop, no_notifications=True)

    @classmethod
    def teardown_class(cls):
        asyncio.run(cls.rec.close())

    @pytest.mark.asyncio
    async def test_get_user_id(self):
        actual = await self.rec.get_user_id("esl_csgo")
        assert actual.id == ESL_CSGO_ID

