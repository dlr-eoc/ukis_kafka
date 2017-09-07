# encoding: utf8
'''message handlers to handle incomming messages in a consumer'''

class BaseMessageHandler(object):
    '''base message handler to inherit from'''

    def sanitize_value(self, name, value):
        '''override this method in subclasses to correct/fix/... incomming values'''
        return value

    def handle_message(self, cur, data):
        '''handle a message. this method should raise an exception on error
           to propagate to an eventual transaction management in the consumer.
           
           This method should return a boolean indicating if the message has been
           processed successfuly (true, false if not)'''
        raise NotImplementedError('needs to be implemented in subclasses')
