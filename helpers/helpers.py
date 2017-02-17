from aiohttp import ClientSession
from asyncio import get_event_loop
from async_timeout import timeout
from random import choice


async def fetch(session, n):
    word_site = "http://svnweb.freebsd.org/csrg/share/dict/words?view=co"
    with timeout(10):
        async with session.get(word_site) as response:
            content = await response.text()
            WORDS = content.splitlines()
            return ' '.join([choice(WORDS) for i in range(0, n)])


async def set_session(loop, n):
    async with ClientSession(loop=loop) as session:
        html = await fetch(session, n)
        return html


def get_words(n=10):
    loop = get_event_loop()
    return loop.run_until_complete(set_session(loop, n))
