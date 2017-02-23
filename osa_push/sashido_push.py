from aiohttp import ClientSession
from asyncio import get_event_loop
from async_timeout import timeout


async def push(app_id, rest_key, url, message):
    async with ClientSession() as session:
            head = {
                "X-Parse-Application-Id": app_id,
                "X-Parse-REST-API-Key": rest_key,
                "Content-Type": "application/json"
            }

            async with session.post(url, data=message, headers=head) as resp:
                return resp.status, await resp.text()


def send_push(app_id, rest_key, url, message):
    loop = get_event_loop()
    return loop.run_until_complete(push(app_id,
                                        rest_key,
                                        url,
                                        message))
