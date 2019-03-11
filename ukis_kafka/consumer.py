# encoding: utf8

from kafka import KafkaConsumer

from .wireformat import basic_from_wireformat
from .wireformat import pack

from psycopg2.extensions import TRANSACTION_STATUS_UNKNOWN

import logging
import collections
import time

logger = logging.getLogger(__name__)

class BaseConsumer(KafkaConsumer):
    '''base class for all consumers in the package'''

    # maps a topic to a list of handlers
    topic_handlers = collections.defaultdict(list)

    def __init__(self, *topics, **config):
        if not config.get('max_poll_records', None):
            config['max_poll_records'] = 100
        config['value_deserializer'] = basic_from_wireformat
        config['key_deserializer'] = pack.unpack
        super(BaseConsumer, self).__init__(*topics, **config)

    def register_topic_handler(self, topic, handler):
        self.topic_handlers.setdefault(topic, []).append(handler)
        self._do_subscribe()

    def _do_subscribe(self):
        if self.topic_handlers:
            self.subscribe(list(self.topic_handlers.keys()))

    def consume(self):
        '''default implementation. may be overriden in subclasses'''
        while True:
            messages = self.poll(timeout_ms=20*1000)

            if messages:
                count_handled = 0
                count_dropped = 0
                for topicpartition, msglist in list(messages.items()):
                    handlers = self.topic_handlers[topicpartition.topic]
                    for msg in msglist:
                        data = msg.value
                        logger.debug('Handling message on topic={0}'.format(topicpartition.topic))
                        count_handled += 1
                        try:
                            if data is None:
                                raise Exception("message contains no data")
                            for handler in handlers:
                                handler.handle_message(data)
                        except Exception as e:
                            count_dropped += 1
                            logger.error("Message dropped because of error: {0}".format(e))

                logger.info('Handled {0} message(s), of which {1} have been dropped because of errors'.format(
                                    count_handled, count_dropped))

                self.commit()


class PostgresqlConsumer(BaseConsumer):
    '''consume arriving messages into a postgresql database.
       
       provides transaction management between kafka and postgresql'''

    conn = None # psycopg2 connection
    resubscription_interval_seconds = None

    def __init__(self, conn, *topics, **config):
        '''
        parameters:
            conn: psycopg2 connection instance
        '''
        # This class handles synchronizing between kafka and postgresql commits itself
        config['enable_auto_commit'] = False

        self.resubscription_interval_seconds = config.pop('resubscription_interval_seconds', None)
        super(PostgresqlConsumer, self).__init__(*topics, **config)
        self.conn = conn

    def is_connection_alive(self):
        return self.conn.get_transaction_status() != TRANSACTION_STATUS_UNKNOWN

    def consume(self):
        cur = self.conn.cursor()

        # collect the schema information in all handlers
        for handlers in list(self.topic_handlers.values()):
            for handler in handlers:
                if hasattr(handler, 'analyze_schema'):
                    handler.analyze_schema(cur)

        # resubscribe peridocialy when no messages are received
        # to avoid a state where no messages are distributed to this
        # consumer
        resubscription_time = None

        while True:
            messages = self.poll(timeout_ms=20*1000)

            if not self.is_connection_alive():
                raise IOError('The connection with the database server is broken')

            if messages:
                if self.resubscription_interval_seconds is not None:
                    resubscription_time = time.time() + self.resubscription_interval_seconds # connection is alive and well

                count_handled = 0
                count_dropped = 0
                for topicpartition, msglist in list(messages.items()):
                    handlers = self.topic_handlers[topicpartition.topic]
                    for msg in msglist:
                        data = msg.value
                        logger.debug('Handling message on topic={0}'.format(topicpartition.topic))
                        count_handled += 1
                        try:
                            cur.execute("savepoint current_msg")
                            for handler in handlers:
                                cur.execute("savepoint current_msg_handler")
                                success = handler.handle_message(cur, data)
                                if success == False:
                                    cur.execute("rollback to savepoint current_msg_handler")
                                else:
                                    # success == None means the handler does not support
                                    # returning the success-flag, so we need to asume
                                    # it was a success
                                    cur.execute("release savepoint current_msg_handler")
                            cur.execute("release savepoint current_msg")
                        except Exception as e:
                            cur.execute("rollback to savepoint current_msg")
                            count_dropped += 1
                            logger.error("Message dropped because of failure to insert: {0}, properties: {1}".format(e, data['properties']))

                logger.info('Handled {0} message(s), of which {1} have been dropped because of errors'.format(
                                    count_handled, count_dropped))

                self.conn.commit()
                self.commit()

            else:
                if resubscription_time is not None and resubscription_time <= time.time():
                    logger.info('No messages have benn received for {0} seconds - resubscribing to all topics'.format(
                                self.resubscription_interval_seconds))
                    self.unsubscribe()
                    self._do_subscribe()
                    resubscription_time = time.time() + self.resubscription_interval_seconds
