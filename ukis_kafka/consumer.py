# encoding: utf8

from kafka import KafkaConsumer

from wireformat import basic_from_wireformat
from .wireformat import pack

from psycopg2.extensions import TRANSACTION_STATUS_UNKNOWN

import logging

logger = logging.getLogger(__name__)

class BaseConsumer(KafkaConsumer):
    '''base class for all consumers in the package'''

    topic_handlers = {}

    def register_topic_handler(self, topic, handler):
        self.topic_handlers[topic] = handler
        self.subscribe(self.topic_handlers.keys())

    def consume(self):
        raise NotImplementedError('needs to implemented in subclasses')


class PostgresqlConsumer(BaseConsumer):
    '''consume arriving messages into a postgresql database.
       
       provides transaction management between kafka and postgresql'''

    conn = None # psycopg2 connection

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

    def is_connection_alive(self):
        return self.conn.get_transaction_status() != TRANSACTION_STATUS_UNKNOWN

    def consume(self):
        cur = self.conn.cursor()

        # collect the schema information in all handlers
        for handler in set(self.topic_handlers.values()):
            handler.collect_schema(cur)

        while True:
            messages = self.poll(timeout_ms=20*1000)

            if not self.is_connection_alive():
                raise IOError('The connection with the database server is broken')

            if messages:
                count_handled = 0
                count_dropped = 0
                for topicpartition, msglist in messages.items():
                    handler = self.topic_handlers[topicpartition.topic]
                    for msg in msglist:
                        data = msg.value
                        logger.debug('Handling message on topic={0}'.format(topicpartition.topic))
                        count_handled += 1
                        try:
                            cur.execute("savepoint current_msg")
                            handler.handle_message(cur, data)
                            cur.execute("release savepoint current_msg")
                        except Exception, e:
                            cur.execute("rollback to savepoint current_msg")
                            count_dropped += 1
                            logger.error("Message dropped because of failure to insert: {0}, properties: {1}".format(e, data['properties']))

                logger.info('Handled {0} message(s), of which {1} have been dropped because of errors'.format(
                                    count_handled, count_dropped))

                self.conn.commit()
                self.commit()
