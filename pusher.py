
from boto.sqs import connect_to_region
from boto.sqs.message import Message as botoMessage

from osa_push.sashido_message import validate_message, dict_to_json
from osa_push.osa_constants import WHERE_SCHEMA, DATA_SCHEMA, KEYS_PUSH
from push_config import *


def _get_queue():
    conn = connect_to_region(
        'us-west-1',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    my_queue = conn.get_queue(QUEUE)
    return my_queue


def send_to_SQS(msj):
    """
    Due the need of python 2.7, the whole myqueue method has to be redo due
    the async calls
    """
    q = _get_queue()
    if q:
        m = botoMessage()
        m.set_body(msj)
        q.write(m)
    else:
        raise NameError('Queue not found')
        exit(1)

def digest_msj(*data):
    # data contains the where and content dicts
    jsons = dict_to_json(KEYS_PUSH, *data)
    msj = {}
    for k, d in zip(KEYS_PUSH, jsons):
        msj[k] = d[k]

    data = str(msj).replace("True", "true") \
        .replace("'", "\"")  # Due convertion python -> json
    push_msj = validate_message(data, KEYS_PUSH, (WHERE_SCHEMA, DATA_SCHEMA))

    if push_msj:
        msj = str(data)
        print("--->", data)
        send_to_SQS(msj)


if __name__ == '__main__':
    digest_msj()
