import asyncio
from boto.sqs import connect_to_region
from boto.sqs.message import Message as botoMessage
import warnings

from aiohttp import ClientSession
from asyncio import get_event_loop
from async_timeout import timeout
import concurrent.futures

from osa_push.osa_constants import KEYS_PUSH, WHERE_SCHEMA, DATA_SCHEMA
# from osa_push.sashido_push import push
from osa_push.sashido_message import validate_message, is_boto_message


class MyQueue(object):
    def __init__(self, q_name, region='us-west-1', aws_access_key=None,
                 aws_secret_key=None):
        if aws_access_key and aws_secret_key:
            self.queue = self._get_queue(region, q_name, aws_access_key,
                                         aws_secret_key)
        else:
            self.queue = self._get_queue(region, q_name)
        self.message_list = asyncio.Queue()

    def _get_queue(self, region, q_name, access=None, secret=None):
        """
        Function that connects to AWS SQS service and tries to get a queue
        args:
            queue_name: Name of the queue that will try to get
        return:
            SQS queue object
        """
        if access and secret:
            conn = connect_to_region(
                region,
                aws_access_key_id=access,
                aws_secret_access_key=secret
            )
        else:
            warnings.warn("Using default aws_access_key and aws_secret_key")
            conn = connect_to_region(region)
        my_queue = conn.get_queue(q_name)
        return my_queue

    def validate_queue(self):
        flag = False
        if self.queue:
            flag = True
        return flag

    async def add_message(self, m):
        await self.message_list.put(m)

    async def get_messages(self):
        while True:
            new_messages = self.queue.get_messages(num_messages=10,
                                                   visibility_timeout=90,
                                                   wait_time_seconds=20)
            for m in new_messages:
                await self.add_message(m.get_body())
                self.queue.delete_message(m)
            print('Got {} new messages'.format(len(new_messages)))
            sleep_time = len(new_messages)*5
            sleep = sleep_time if sleep_time <= 60 else 60
            await asyncio.sleep(sleep)

    def delete_from_queue(self, m):
        self.queue.delete_message(m)

    async def send_to_queue(self, m):
        if is_boto_message(m):
            await self.queue.write(m.message)
            self.delete_from_queue(m)



    async def push(self, app_id, rest_key, url, message, loop):
        async with ClientSession(loop=loop) as session:
            with timeout(20):
                head = {
                    "X-Parse-Application-Id": app_id,
                    "X-Parse-REST-API-Key":  rest_key,
                    "Content-Type": "application/json"
                }
                print("pushing message: ", message)
 
                async with session.post(url,
                                        data=message,
                                        headers=head) as resp:
                    return resp.status, await resp.text()

    async def digest_messages(self, sashido_keys, loop):
        while True:
            if not self.message_list.empty():
                value = await self.message_list.get()
                # validate message
                validated_msj = validate_message(value.replace("True", "true"),
                                                 KEYS_PUSH,
                                                 (WHERE_SCHEMA, DATA_SCHEMA))
                print("validated?" )
                if validated_msj:
                    print(validated_msj)
                    # send message
                    validated_msj = validated_msj.replace("True", "true") \
                                                 .replace("'", "\"")
                    # print('--->', validated_msj)
                    api_key, rest_key, url = sashido_keys
                    status = cont = 0
                    while status != 200 or cont >10:
                        try:
                            cont = cont + 1
                            status, txt = await self.push(api_key, rest_key,
                                                          url,
                                                          validated_msj, loop)
                        except concurrent.futures.TimeoutError:
                            print("timeout... retry")
                            if cont == 10:
                                print("TIMEOUT: ", validated_msj)
                    print("ccc>",validated_msj, status, txt, "<ooo", sep='\t')
            await asyncio.sleep(5)
