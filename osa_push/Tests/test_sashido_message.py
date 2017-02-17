import pytest
from boto.sqs.message import Message as botoMessage
from osa_push.sashido_message import (validate_schema, is_json,
                                      convert_to_dict, validate_data,
                                      validate_message, is_boto_message)


def test_is_boto_message_valid():
    m = botoMessage()
    assert is_boto_message(m) is True


def test_is_boto_message_invalid():
    with pytest.raises(TypeError) as execinfo:
        is_boto_message('foo')
    assert "Object m is not type boto.sqs.message.Message(), " \
        "instead is <class 'str'>" == str(execinfo.value)


def test_valid_validate_schema(valid_schema):
    data = {'a': 1, 'b': 'test'}
    assert validate_schema(valid_schema, data) is True


def test_require_validate_schema(valid_schema):
    data = {'a': 1}
    assert validate_schema(valid_schema, data) is False


def test_valid_require_validate_schema(valid_schema):
    data = {'b': 'test string'}
    assert validate_schema(valid_schema, data) is True


def test_invalid_data_validate_schema(valid_schema):
    data = {'a': str(1), 'b': str(2)}
    data_2 = {'a': int(str(1)), 'b': 2}
    assert validate_schema(valid_schema, data) is False
    assert validate_schema(valid_schema, data_2) is False


"""
is_json
"""


def test_valid_is_json(keys):
    data_in = '{"b": [{"6": 6, "5": 5}], "a": [{"1": 1}, {"2": 2}]}'
    data_out = [[{'1': 1}, {'2': 2}], [{'6': 6, '5': 5}]]
    assert is_json(data_in, keys) == data_out


# @pytest.mark.xfail(raises=ValueError)
# use @ with a check function like documenting unfixed bugs
def test_error_is_json(keys):
    assert is_json('(1,2)', keys) is None  # ValueError
    assert is_json('{"a":1, "c":2}', keys) is None  # KeyError
    assert is_json('{"a":1}', keys) is None  # len != keys


"""
convert_to_dict
"""


def test_valid_convert_to_dict():
    obj = [{'a': 1}, {'b': 2}, {'c': 4}, {'d': 5}]
    obj_out = {'a': 1, 'b': 2, 'c': 4, 'd': 5}
    assert convert_to_dict(obj) == obj_out


def test_invalid_convert_to_dict():
    obj = {'a': 1}
    assert convert_to_dict(obj) is None


"""
validate_data
"""


def test_valid_validate_data(keys, complex_schemas):
    data = [[{"sub_a_1": True}, {"sub_a_2": 1}],
            [{"sub_b_1": False}, {"sub_b_2": 2}]]
    data_out = {"a": {"sub_a_1": True, "sub_a_2": 1},
                "b": {"sub_b_1": False, "sub_b_2": 2}}

    assert validate_data(data, keys, complex_schemas) == str(data_out)


def test_invalid_validate_data(keys, complex_schemas):
    # sub_a_3 instead of _2
    data = [[{"sub_a_1": True}, {"sub_a_3": 1}],
            [{"sub_b_1": False}, {"sub_b_2": 2}]]

    assert validate_data(data, keys, complex_schemas) is None


"""
validate_message
"""


def test_valid_validate_message(keys, complex_schemas):
    data = '{"a": [{"sub_a_1": true}, {"sub_a_2": 1}], \
    "b": [{"sub_b_1": false},{"sub_b_2":2}]}'

    data_out = str({'a': {'sub_a_1': True, 'sub_a_2': 1},
                    'b': {'sub_b_1': False, 'sub_b_2': 2}})

    assert validate_message(data, keys, complex_schemas) == data_out
