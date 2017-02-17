from boto.sqs.message import Message as botoMessage


class MyMessage(object):
    def __init__(self, where, data, message=botoMessage()):
        self.message = message
        self.where = where
        self.data = data

    def __str__(self):
        return self.message.get_body()

    def build_message(self):
        body = {'where': self.where, 'data': self.data}
        self.message.set_body(str(body).replace('\'', '"'))
