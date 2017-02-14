import pytest

from ..sashido_message import (validate_schema, is_json, convert_to_dict,
                               validate_data, validate_message)

def get_valid_schema():
    schema = {'a':{'type':'integer'},
              'b':{'type':'string', 'required':True}
             }
    return schema

def get_keys():
    keys = ['a', 'b']
    return keys

def test_valid_validate_schema():
    data = {'a':1, 'b':'test'}
    assert validate_schema(get_valid_schema(), data) == True

def test_require_validate_schema():
    data = {'a':1}
    assert validate_schema(get_valid_schema(), data) == False

def test_valid_require_validate_schema():
    data = {'b': 'test string'}
    assert validate_schema(get_valid_schema(), data) == True

def test_invalid_data_validate_schema():
    data = {'a': str(1), 'b': str(2)}
    data_2 = {'a': int(str(1)), 'b': 2}
    assert validate_schema(get_valid_schema(), data) == False
    assert validate_schema(get_valid_schema(), data) == False

"""
is_json
"""

def test_valid_is_json():
    data_in = '{"b": [{"6": 6, "5": 5}], "a": [{"1": 1}, {"2": 2}]}'
    data_out= [[{'1': 1}, {'2': 2}],[{'6': 6, '5': 5}]]
    assert is_json(data_in, get_keys()) == data_out

# @pytest.mark.xfail(raises=ValueError)
# use @ with a check function like documenting unfixed bugs
def test_error_is_json():
    assert is_json('(1,2)',get_keys()) == None  # ValueError
    assert is_json('{"a":1, "c":2}', get_keys()) == None  # KeyError
    assert is_json('{"a":1}', get_keys()) == None  # len != keys


"""
convert_to_dict
"""
def test_valid_convert_to_dict():
    obj = [{'a':1}, {'b':2}, {'c':4}, {'d':5}]
    obj_out = {'a': 1, 'b': 2, 'c': 4, 'd': 5}
    assert convert_to_dict(obj) == obj_out


def test_invalid_convert_to_dict():
    obj = {'a':1}
    assert convert_to_dict(obj) == None


"""
validate_data
"""
def test_valid_validate_data():
    data = [[{"sub_a_1": True}, {"sub_a_2": 1}],
            [{"sub_b_1": False},{"sub_b_2":2}]]
    keys = ['a', 'b']
    a_schema = {'sub_a_1':{'type':'boolean'}, 'sub_a_2':{'type':'integer'}}
    b_schema = {'sub_b_1':{'type':'boolean'}, 'sub_b_2':{'type':'integer'}}
    schemas = (a_schema, b_schema)

    data_out = {"a": {"sub_a_1": True, "sub_a_2": 1},
                "b": {"sub_b_1": False, "sub_b_2": 2}}

    assert validate_data(data, keys, schemas) == str(data_out)


def test_invalid_validate_data():
    # sub_a_3 instead of _2
    data = [[{"sub_a_1": True}, {"sub_a_3": 1}],
            [{"sub_b_1": False},{"sub_b_2":2}]]
    keys = ['a', 'b']
    a_schema = {'sub_a_1':{'type':'boolean'}, 'sub_a_2':{'type':'integer'}}
    b_schema = {'sub_b_1':{'type':'boolean'}, 'sub_b_2':{'type':'integer'}}
    schemas = (a_schema, b_schema)

    assert validate_data(data, keys, schemas) == None

"""
validate_message
"""
def test_valid_validate_message():
    keys = ['a', 'b']
    a_schema = {'sub_a_1':{'type':'boolean'}, 'sub_a_2':{'type':'integer'}}
    b_schema = {'sub_b_1':{'type':'boolean'}, 'sub_b_2':{'type':'integer'}}
    schemas = (a_schema, b_schema)

    data = '{"a": [{"sub_a_1": true}, {"sub_a_2": 1}], \
    "b": [{"sub_b_1": false},{"sub_b_2":2}]}'

    data_out = str({'a': {'sub_a_1': True, 'sub_a_2':1},
                    'b': {'sub_b_1': False, 'sub_b_2': 2}})

    assert validate_message(data, keys, schemas) == data_out
