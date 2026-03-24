#standards imports
import traceback, logging

#imports to use AMQP 1.0 communication protocol
from proton.handlers import MessagingHandler
from proton.reactor import Container

class Receiver():
    def __init__(self, on_message_callback=None):
        super(Receiver, self).__init__()
        self.on_message_callback = on_message_callback

    def receive_event(self, server, topic):
        handler = self.event_Receiver_handller(server, topic, self.on_message_callback)
        Container(handler).run()

    class event_Receiver_handller(MessagingHandler):
        def __init__(self, server, topic, on_message_callback=None):
            super(Receiver.event_Receiver_handller, self).__init__()
            self.server = server
            self.topic = topic
            self.on_message_callback = on_message_callback
            logging.info("Agent will start listening for events in the topic: {}".format(self.topic))

        def on_start(self, event):
            conn = event.container.connect(self.server)
            event.container.create_receiver(conn, self.topic)

        def on_message(self, event):
            try:
                # Call the custom on_message callback if provided
                if self.on_message_callback:
                    self.on_message_callback(event)

            except Exception:
                traceback.print_exc()
