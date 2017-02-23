
from osa_push.sashido_message import validate_message, dict_to_json
from osa_push.osa_constants import WHERE_SCHEMA, DATA_SCHEMA, KEYS_PUSH


def send_to_SQS(*data):
    # data contains the where and content dicts
    jsons = dict_to_json(KEYS_PUSH, *data)
    msj = {}
    for k, d in zip(KEYS_PUSH, jsons):
        msj[k] = d[k]

    data = str(msj).replace("True", "true") \
        .replace("'", "\"")  # Due convertion python -> json
    push_msj = validate_message(data, KEYS_PUSH, (WHERE_SCHEMA, DATA_SCHEMA))

    return push_msj


if __name__ == '__main__':
    send_to_SQS()
