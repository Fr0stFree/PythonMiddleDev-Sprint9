from aiohttp import ClientSession

from aiochclient import ChClient


class EventLoader:
    async def load(self, values) -> None:
        async with ClientSession() as s:
            client = ChClient(s)
            await client.execute('INSERT INTO default.events (*) VALUES', *values)
