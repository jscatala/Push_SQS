from cerberus import Validator
from json import loads


# format
# {"where":[
#          {"notification_enabled":true},
#          {"channels":["osa"]},
#          {"role_id": 1},
#          {"team_id": 1},
#          ],
#  "data":[
#         {"title":"asdfg"},
#         {"alert":"asdfg"},
#         {"status":"ACTIVE"},
#         {"task":"asdfg"},
#         {"user":1},
#         {"team_id":1},
#         {"team":1}
#         ]}


def validate_data(content, keys, schemas):
    from functools import reduce

    # Find a way that validate_schema is called before convert_to_dict
    schema_dict = dict([(k, d) for k, d in zip(keys, schemas)])
    content = dict([(k, convert_to_dict(i)) for k, i in zip(keys, content)])
    f = lambda x: validate_schema(schema_dict[x], content[x])
    validation = map(f, keys)
    if reduce(lambda a, x: x and a, validation):
        message = str(content)
    else:
        message = None
    return message


def validate_schema(schema, conditions):
    v = Validator(schema)
    return v.validate(conditions)


def is_json(message, keys):
    try:
        json_object = loads(message)
        if len(json_object) != len(keys):
            return None
        data = [json_object[key] for key in keys]
    except ValueError as e:
        return None
    except KeyError as k:
        return None
    return data


def convert_to_dict(obj):
    try:
        data = dict([dic.popitem() for dic in obj])
    except AttributeError as e:
        data = None
    return data


def validate_message(body, keys, schemas):
    message = None
    body_json = is_json(body, keys)
    if body_json:
        message = validate_data(body_json, keys, schemas)
    return message
