from measurement_plane.messaging.message_format import MessageFields
from datetime import datetime
import hashlib

class Message:
    def __init__(self) -> None:
        self.message = {}
        self.construct_base()
    
    def construct_base(self):
        self.message = {
            MessageFields.LABEL: None,
            MessageFields.ENDPOINT: None,
            MessageFields.CAPABILITY_NAME: None,
            MessageFields.PARAMETERS_SCHEMA: None,
            MessageFields.RESULT_SCHEMA: None,
            MessageFields.TIMESTAMP: None,
            MessageFields.NONCE: None,
            MessageFields.METADATA: None
        }
    
    @staticmethod
    def calculate_capability_id(message):
        try:
            endpoint = message["endpoint"]
            capability_name = message["capabilityName"]
            combined_string = Message.combine_to_string([endpoint, capability_name])
            capability_id = hashlib.sha256(combined_string.encode()).hexdigest()
            return capability_id
        except KeyError as e:
            raise KeyError(f"Missing required field: {e}")
        except Exception as e:
            raise Exception(f"An error occurred while calculating capability ID: {e}")

    @staticmethod
    def calculate_measurement_id(message):
        try:
            capability_id = Message.calculate_capability_id(message)
            parameters = message["parameters"]
            schedule = message["schedule"]
            combined_string = Message.combine_to_string([capability_id, parameters, schedule])
            measurement_id = hashlib.sha256(combined_string.encode()).hexdigest()
            return measurement_id
        except KeyError as e:
            raise KeyError(f"Missing required field: {e}")
        except Exception as e:
            raise Exception(f"An error occurred while calculating measurement ID: {e}")

    @staticmethod
    def calculate_operation_id(message):
        try:
            measurement_id = Message.calculate_measurement_id(message)
            nonce = message["nonce"]
            timestamp = message["timestamp"]
            combined_string = Message.combine_to_string([measurement_id, nonce, timestamp])
            operation_id = hashlib.sha256(combined_string.encode()).hexdigest()
            return operation_id
        except KeyError as e:
            raise KeyError(f"Missing required field: {e}")
        except Exception as e:
            raise Exception(f"An error occurred while calculating operation ID: {e}")

    @staticmethod
    def combine_to_string(attributes: list) -> str:
        combined_string = ""
        for att in attributes:
            # Convert dictionary to string, remove spaces and newlines
            att_str = str(att).replace(" ", "").replace("\n", "")
            combined_string += att_str
        return combined_string

class CapabilityMessage(Message):
    def __init__(self) -> None:
        super().__init__()
    
    def construct(self, label=None, endpoint = None, capability_name = None ,parameters_schema=None, result_schema=None, plot_schema=None, nonce = None, metadata = None, type = None):
        self.message[MessageFields.LABEL] = label
        self.message[MessageFields.ENDPOINT] = endpoint
        self.message[MessageFields.CAPABILITY_NAME] = capability_name
        self.message[MessageFields.PARAMETERS_SCHEMA] = parameters_schema
        self.message[MessageFields.RESULT_SCHEMA] = result_schema
        self.message[MessageFields.PLOT_SCHEMA] = plot_schema
        self.message[MessageFields.TIMESTAMP] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-4]
        self.message[MessageFields.NONCE] = nonce
        self.message[MessageFields.METADATA] = metadata
        self.message[MessageFields.CAPABILITY] = type

