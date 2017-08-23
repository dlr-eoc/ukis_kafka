# encoding: utf8

from kafka import KafkaConsumer

from wireformat.basic import basic_from_wireformat
from .wireformat import pack

from . import pg

import logging

logger = logging.getLogger(__name__)


class ToPostgisConsumer(KafkaConsumer):
    '''consume arriving messages in to a postigs database'''

    conn = None # psycopg2 connection
    topic_handlers = {}

    def __init__(self, conn, *topics, **config):
        '''
        parameters:
            conn: psycopg2 connection instance
        '''
        if not config.get('max_poll_records', None):
            config['max_poll_records'] = 100
        config['value_deserializer'] = basic_from_wireformat
        config['key_deserializer'] = pack.unpack

        # This class handles synchronizing between kafka and postgresql commits itself
        config['enable_auto_commit'] = False

        super(self.__class__, self).__init__(*topics, **config)
        self.conn = conn

    def register_topic_handler(self, topic, handler):
        self.topic_handlers[topic] = handler
        self.subscribe(self.topic_handlers.keys())

    def consume(self):
        cur = self.conn.cursor()

        # collect the schema information in all handlers
        for handler in set(self.topic_handlers.values()):
            handler.collect_schema(cur)

        while True:
            messages = self.poll(timeout_ms=20*1000)
            if messages:
                for topicpartition, msglist in messages.items():
                    handler = self.topic_handlers[topicpartition.topic]
                    for msg in msglist:
                        data = msg.value
                        logger.info('handling message on topic={0}'.format(topicpartition.topic))
                        try:
                            cur.execute("savepoint current_msg")
                            handler.handle_message(cur, data)
                            cur.execute("release savepoint current_msg")
                        except Exception, e:
                            cur.execute("rollback to savepoint current_msg")
                            logger.error("message dropped because of failure to insert: {0}: {1}".format(e, props))

                self.conn.commit()
                self.commit()



