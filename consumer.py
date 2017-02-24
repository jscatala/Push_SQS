import asyncio

from osa_push.push_myqueue import MyQueue
from push_config import (AWS_ACCESS_KEY, AWS_SECRET_KEY, QUEUE,
                         PARSE_URL, APPLICATION_ID, REST_API_KEY)  # , ERROR_QUEUE


def digest_SQS():
    ERROR__QUEUE_NOT_FOUND = 'Queue not found. Check queue name and retry'

    q = MyQueue(QUEUE, 'us-west-1', AWS_ACCESS_KEY, AWS_SECRET_KEY)
    if q.validate_queue():
        # error_queue = MyQueue(AWS_ACCESS_KEY, AWS_SECRET_KEY, ERROR_QUEUE)
        loop = asyncio.get_event_loop()

        for i in range(3):
            loop.create_task(q.get_messages())

        for i in range(6):
            loop.create_task(q.digest_messages((APPLICATION_ID, REST_API_KEY,
                                                PARSE_URL), loop))

        loop.run_forever()
    else:
        raise NameError(ERROR__QUEUE_NOT_FOUND)
        exit(1)


if __name__ == '__main__':
    digest_SQS()
