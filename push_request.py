import boto.sqs
from boto.sqs.message import Message
import curio
import curio_http
from json import loads, dumps
from sys import exit

from push_config import *


# too many concurrent connections?
# MAX_CONNECTIONS_PER_HOST = N
# sema = curio.BoundedSemaphore(MAX_CONNECTIONS_PER_HOST)


async def send_push_async(conditions, content):
    """
    Send data to sashido API.
    args:
        conditions: conditions used to send the message
        content: data that the message will have
    valid message format:
        {
        "where":
            {
            "notifications_enabled": true,
            "channel": "osa"
            },
        "data":
            {
            "title": "prueba push desde sqs para python, mensaje 3",
            "alert": "Nuevo Mensaje SQS", "status": "Nuevo Mensaje SQS"
            }
        }
    """
    # async with sema, ...
    async with curio_http.ClientSession() as session:
        head = {
            "X-Parse-Application-Id": APPLICATION_ID,
            "X-Parse-REST-API-Key": REST_API_KEY,
            "Content-Type": "application/json"
        }
        body = dumps({
            "where": conditions,
            "data": content
        })
        response = await session.post(PARSE_URL, headers=head, data = body)
        content = await response.json()
        return response.status_code, content


async def move_from_queue(message, from_q, to_q=None):
    """
    Move or remove message from_q to to_q if exists
    args:
        message: sqs.message object
        from_q: queue where the message came
        to_q: queue where the message might be placed
    """
    from_q.delete_message(message)
    if to_q:
        to_q.write(message)


async def digest_message(message, queue, error_queue):
    """
    Digest a valid message, and send it to sashido API
    args:
        message: sqs.message object retrieved from queue
        queue: queue from where the message got pulled
        error_queue: queue where to move the message (might be used on the
                    future)
    """
    def convert_to_dict(obj , key):
        return dict([ dic.popitem() for dic in obj[key]])
    get_body = loads(message.get_body())
    data = convert_to_dict(get_body, 'data')
    constrains = convert_to_dict(get_body, 'where')
    if constrains['notifications_enabled'] == 'true':  # json true -> python True
        constrains['notifications_enabled'] = True
    status, content = await send_push_async(constrains, data)
    if status != 200:
        print('[ERROR]: digest_message')
        print(body)
        print('Status code: ', status)
        print('Content: ', content)
        print()
    await move_from_queue(message, queue, None)

def valid_format(m):
    """
    Verify if the format of the message does have data and conditions (where)
    args:
        m: message.get_body() data
    return:
        function: function where the message has to be digested
    """
    from functools import reduce
    try:
        body = loads(m)
        data = ['data', 'where']
        f = lambda x: True if body[x] is not None or  body[x] is not '' else False
        if reduce(lambda a,x: a and x, map(f, data)):
            function = 'digest'
    except Exception as e:
        print('Message does not have a valid format')
        print(m)
        print(e)
        print()
        function = 'delete'

    return function


def send_to(m):
    """
    Depends if the message is valid or not, sends to digest or delete from main
    queue the message object
    args:
        m: sqs.message object
    """
    functions = {'delete': move_from_queue, 'digest': digest_message}
    function = valid_format(m.get_body())
    curio.run(functions[function](m, q, error_queue))


def get_queue(queue_name):
    """
    Function that connects to AWS SQS service and tries to get a queue
    args:
        queue_name: Name of the queue that will try to get
    return:
        SQS queue object
    """
    conn = boto.sqs.connect_to_region(
        'us-west-1',
        aws_access_key_id = AWS_ACCESS_KEY,
        aws_secret_access_key = AWS_SECRET_KEY
    )
    my_queue = conn.get_queue(queue_name)
    return my_queue


if __name__ == '__main__':
    q = get_queue(QUEUE)
    if q:  # if main queue is None
        error_queue = get_queue(ERROR_QUEUE)
        while True:
            messages = q.get_messages()
            if len(messages) > 0:
                [ send_to(m) for m in messages ]
    else:
        print("Queue not found. Shutting down")
        exit(1)
