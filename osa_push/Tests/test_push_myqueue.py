import unittest
from mock import patch, Mock

from osa_push.push_myqueue import MyQueue


class MyQueueTestCase(unittest.TestCase):
    patch_queue = patch.object(MyQueue, '_get_queue')

    @patch_queue
    def test_queue_init(self, get_queue_mock):
        q = MyQueue('foo')
        get_queue_mock.assert_called_once_with('us-west-1', 'foo')
        assert q.queue == get_queue_mock.return_value

    @patch_queue
    def test_queue_with_keys(self, get_queue_mock):
        q = MyQueue('foo', aws_access_key='access_key',
                    aws_secret_key='secret_key')
        get_queue_mock.assert_called_once_with('us-west-1', 'foo',
                                               'access_key', 'secret_key')
        assert q.queue == get_queue_mock.return_value

    @patch_queue
    def test_queue_init_with_region(self, get_queue_mock):
        q = MyQueue('foo', 'us-east-1')
        get_queue_mock.assert_called_once_with('us-east-1', 'foo')
        assert q.queue == get_queue_mock.return_value

    @patch('osa_push.push_myqueue.connect_to_region')
    def test_get_queue(self, connect_to_region_mock):
        sqs_connection_mock = Mock()
        sqs_connection_mock.get_queue.return_value = 'queue'

        connect_to_region_mock.return_value = sqs_connection_mock

        q = MyQueue('foo', aws_access_key=None, aws_secret_key=None)
        assert connect_to_region_mock.called
        assert q.queue == 'queue'
        sqs_connection_mock.get_queue.assert_called_with('foo')

        q = MyQueue('foo', aws_access_key='access', aws_secret_key='secret')
        assert connect_to_region_mock.called
        assert q.queue == 'queue'
        sqs_connection_mock.get_queue.assert_called_with('foo')

    @patch_queue
    def test_validate_queue(self, get_queue_mock):
        q = MyQueue('foo')
        get_queue_mock.assert_called_once_with('us-west-1', 'foo')
        assert q.validate_queue() is True

    @patch_queue
    def test_validate_queue_invalid(self, get_queue_mock):
        get_queue_mock.return_value = False
        q = MyQueue('foo')
        get_queue_mock.assert_called_once_with('us-west-1', 'foo')
        assert q.validate_queue() is False

    def add_message(loop):

