import asyncio
from pytest import fixture


@fixture(scope="module")
def valid_schema():
    schema = {'a': {'type': 'integer'},
              'b': {'type': 'string', 'required': True}}
    return schema


@fixture(scope="module")
def complex_schemas():
    a_schema = {'sub_a_1': {'type': 'boolean'}, 'sub_a_2': {'type': 'integer'}}
    b_schema = {'sub_b_1': {'type': 'boolean'}, 'sub_b_2': {'type': 'integer'}}
    return (a_schema, b_schema)


@fixture(scope="module")
def keys():
    keys = ['a', 'b']
    return keys

@fixture
def loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop

    loop.close()
