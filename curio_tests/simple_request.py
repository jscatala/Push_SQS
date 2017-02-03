import curio
import curio_http


async def main():
    async with curio_http.ClientSession() as session:
        response = await session.get('https://httpbin.org/get')

        print('Status code: ' ,response.status_code)

        content = await response.json()

        print('Content: ', content)

if __name__ == '__main__':
    curio.run(main())
