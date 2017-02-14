import asyncio
from boto.sqs import connect_to_region
from boto.sqs.message import Message as botoMessage

from .osa_constants import KEYS_PUSH, WHERE_SCHEMA, DATA_SCHEMA
from .sashido_push import send_push
from .sashido_message import validate_message


class MyQueue(object):
    def __init__(self, aws_access_key, aws_secret_key, q_name,
                 region='us-west-1'):
        self.queue = self._get_queue(aws_access_key, aws_secret_key,
                                     region, q_name)
        self.message_list = asyncio.Queue()
        self.counter = 0

    def _get_queue(self, access, secret, region, q_name):
        """
        Function that connects to AWS SQS service and tries to get a queue
        args:
            queue_name: Name of the queue that will try to get
        return:
            SQS queue object
        """
        conn = connect_to_region(
            region,
            aws_access_key_id=access,
            aws_secret_access_key=secret
        )
        my_queue = conn.get_queue(q_name)
        return my_queue

    def _validate_message(self, m):
        if not isinstance(type(m), botoMessage):
            message = 'Object m is not type boto.sqs.message.Message()'
            raise TypeError(message + ', instead is {}'.format(type(m)))
        else:
            return True

    def validate_queue(self):
        flag = False
        if self.queue:
            flag = True
        return flag

    async def add_message(self, m):
        await self.message_list.put(m)

    async def get_message(self):
        return await self.message_list.get()

    async def get_messages(self):
        while True:
            new_messages = self.queue.get_messages(num_messages=10,
                                                   visibility_timeout=90,
                                                   wait_time_seconds=20)
            for m in new_messages:
                await self.add_message(m)
            print('Got {} new messages'.format(len(new_messages)))
            sleep_time = len(new_messages)*5
            sleep = sleep_time if sleep_time <= 60 else 60
            await asyncio.sleep(sleep)

    def delete_from_queue(self, m):
        self.queue.delete_message(m)
        self.counter += 1
        print(self.counter)

    async def send_to_queue(self, m):
        if self._validate_message(m):
            await self.queue.write(m.message)
            self.delete_from_queue(m)

    async def digest_messages(self, sashido_keys):

        while True:
            message = await self.get_message()
            # print('Digester n', i, '-->', self.message_list.qsize() +1)
            # validate message
            validated_msj = validate_message(
                str(message.get_body()).replace("True", "true"),
                KEYS_PUSH,
                (WHERE_SCHEMA, DATA_SCHEMA)
            )
            print("validated_msj", validated_msj)
            if validated_msj:
                # send message
                print('--->', message.get_body())
                api_key, rest_key, url = sashido_keys
                send_push(api_key, rest_key, url,
                          str(validated_msj).replace("true", "True"))
                self.delete_from_queue(message)
            else:
                print('xx-->', message.get_body())
                await asyncio.sleep(5)
