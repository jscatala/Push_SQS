#! -*- coding: utf-8 -*-

import boto.sqs
from boto.sqs.message import Message as MessageBoto
import curio
import curio_http
from random import choice

from push_config import *

N_MESSAGES_TO_SEND = 10


class MessageQueue(object):
    def __init__(self, queue_name):
        self.queue = self._get_queue(queue_name)
        self.message_stack = list()

    def _get_queue(self, name):
        conn = boto.sqs.connect_to_region(
            'us-west-1',
            aws_access_key_id = AWS_ACCESS_KEY,
            aws_secret_access_key = AWS_SECRET_KEY
        )
        return conn.get_queue(name)

    def verify_queue(self):
        is_valid = False
        if self.queue:
            is_valid = True
        return is_valid

    def add_to_stack(self, m):
        self.message_stack.append(m)

    def remove_from_stack(self, m):
        self.message_stack.remove(m)

    def remove_from_queue(self, m):
        self.queue.delete_message(m)

    async def send_to_sqs(self):
        for m in self.message_stack:
            self.queue.write(m.message)
            self.remove_from_stack(m)


class MyMessage(object):
    def __init__(self):
        STATUS_OPS = ('REJECTED', 'ACCEPTED')

        self.where = {
            'notification_enabled': False,
            'channels': list()
        }
        self.data= {
            "title": "",  #Titulo del mensaje push
            "alert": "",  #Descripcion del mensaje Push (iOS)
            "status": "",  # Estado de la tarea
            "task": "",  # Titulo de la tarea
            "user": "",  #Quien ejecuto la accion
            "team_id": 0,  # Id por el cual se actualiza el stack de notiticaciones
            "team": ""  # Llave para agrupar las notificaciones
        }
        self.message = MessageBoto()

    def build_message(self):
        """
        build a Message obj, from where and data
        """
        body = {'where': self.where, 'data': self.data}
        self.message.set_body(str(body).replace('\'','"').replace('True', 'true'))

    def fill_content(self, placeholder, data):
        #has to be fixed
        data_array = [ {k: v} for k,v in data.items()]
        if placeholder == 'data':
            self.data = data_array
        elif placeholder == 'where':
            self.where = data_array

    def __str__(self):
        return self.message.get_body()


def fake_data(words):
    d = {
        "title": words,
        "alert": words,
        "status":"REJECTED",
        "task": words,
        "user": "Usuario",
        "team_id": str(0)
    }
    return d


def fake_where():
    w = {
        "notifications_enabled": True,
        "channels": "osa"
    }
    return(w)


async def get_words():
    import requests

    word_site = "http://svnweb.freebsd.org/csrg/share/dict/words?view=co"
    response = requests.get(word_site)
    WORDS = response.content.splitlines()
    return ' '.join([ choice(WORDS).decode('utf-8') for i in range(0,10)])


async def main (q):
    words = await get_words()
    m = MyMessage()
    m.fill_content('data', fake_data(words))
    m.fill_content('where', fake_where())
    m.build_message()
    q.add_to_stack(m)
    await q.send_to_sqs()


if __name__ == '__main__':
    q = MessageQueue(QUEUE)
    if q.verify_queue():
        curio.run(main(q))
    else:
        print('Queue not Found. Shutting down')
        exit(1)
