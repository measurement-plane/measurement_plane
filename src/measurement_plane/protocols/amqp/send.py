import json
import pickle
import logging
from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container
import numpy as np

def convert_numpy_key(key):
    """
    Converts NumPy-specific types (e.g., numpy.intc) to native Python types.
    """
    if isinstance(key, np.generic):  # Covers numpy.intc and similar types
        return key.item()  # Converts to native Python type (int, float, etc.)
    return key  # If it's already a valid type, return it as-is

def convert_ndarray_to_list(data):
    """
    Recursively converts:
    - Any NumPy arrays in the dictionary to Python lists
    - Any NumPy-specific types used as keys to native Python types
    """
    if isinstance(data, dict):
        return {convert_numpy_key(key): convert_ndarray_to_list(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_ndarray_to_list(item) for item in data]
    elif isinstance(data, np.ndarray):
        return data.tolist()  # Convert NumPy arrays to lists
    else:
        return data  # If it's not a dict, list, or ndarray, return as-is
    
def contains_bytes(data):
    """
    Recursively checks if any part of data contains bytes.
    """
    if isinstance(data, bytes):
        return True
    elif isinstance(data, dict):
        return any(contains_bytes(value) for value in data.values())
    elif isinstance(data, list):
        return any(contains_bytes(item) for item in data)
    return False

class Sender:
    def __init__(self):
        pass

    def send(self, server, topic, messages):
        container = Container(SendHandler(server, topic, messages))
        container.run()

class SendHandler(MessagingHandler):
    def __init__(self, server, topic, messages):
        super(SendHandler, self).__init__()
        self.server = server
        self.topic = topic
        self.messages = messages
        self.confirmed = 0
        self.total = 1

    def on_connection_error(self, event):
        logging.error(f"Connection error while sending messages to server: {self.server} for topic: {self.topic}")
        event.connection.close()

    def on_transport_error(self, event):
        logging.error(f"Transport error while sending messages to server: {self.server} for topic: {self.topic}")
        event.connection.close()

    def on_start(self, event):
        conn = event.container.connect(self.server)
        event.container.create_sender(conn, self.topic)

    def on_sendable(self, event):
        logging.info(f"Agent sending messages to topic {self.topic}")
        try:
            logging.info(f"Message type: {type(self.messages)}")
            if contains_bytes(self.messages):
                logging.info("Message contains bytes; converting entire message to bytes...")
                serialized_message = pickle.dumps(self.messages)
                msg = Message(body=serialized_message)
            else:
                messages_serializable = convert_ndarray_to_list(self.messages)
                
                # Serialize the dictionary to JSON
                msg = Message(body=json.dumps(messages_serializable))
                
            event.sender.send(msg)
            logging.info("Agent sent msg to topic {}".format(self.topic))
            event.sender.close()

        except Exception as e:
            logging.error(f"Error sending messages: {e}")

    

    def on_rejected(self, event):
        logging.error("msg regected while sending msg to server: {} for topic: {}".format(self.server, self.topic))
        return super().on_rejected(event)
        
    def on_accepted(self, event):
        logging.info("msg accepted in topic {}".format(self.topic))
        self.confirmed += 1
        if self.confirmed == self.total:
            event.connection.close()

    def on_disconnected(self, event):
        logging.error("disconnected error while sending msg to server: {} for topic: {}".format(self.server, self.topic))
        self.sent = self.confirmed
