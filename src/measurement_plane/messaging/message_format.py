import hashlib

class Subjects:
    CAPABILITIES_SUBJECT = 'capabilities'

    @staticmethod
    def get_specifications_subject(endpoint: str) -> str:
        return f'{endpoint}.specifications'
    
    @staticmethod
    def get_results_subject(measurement_id: str) -> str:
        return f'{measurement_id}.results'

class MessageFields:
    LABEL = 'label'
    ENDPOINT = 'endpoint'
    SCHEDULE = 'schedule'
    CAPABILITY_NAME = 'capabilityName'
    PARAMETERS = 'parameters'
    PARAMETERS_SCHEMA = 'parameters_schema'
    RESULT_SCHEMA = 'resultSchema'
    PLOT_SCHEMA = 'plotSchema'
    TIMESTAMP = 'timestamp'
    NONCE = 'nonce'
    METADATA = 'metadata'
    CAPABILITY = 'capability'
    SPECIFICATION = 'specification'
    INTERRUPT = 'interrupt'
    RESULT = 'result'
    RESULT_VALUES = 'resultValues'
    RECEIPT = 'receipt'
    EOF_RESULTS = 'EOF_results'

from datetime import datetime, timedelta
import re
class TaskSchedule:
    def __init__(self, schedule:str) -> None:
        self.start, self.stop, self.periodicity, self.stream= None, None, None, None
        self.parse_schedule(schedule)

    def parse_schedule(self, schedule: str):
        parts = schedule.split('|')
        
        # Handle 'now' case
        if parts[0].strip().lower() == 'now':
            self.start = datetime.now()
        else:
            # Parse start time
            try:
                self.start = datetime.fromisoformat(parts[0].strip())
            except ValueError:
                raise ValueError(f"Invalid start time format: {parts[0]}")

        # Parse stop time if provided
        if len(parts) >= 2 and parts[1].strip():
            try:
                self.stop = datetime.fromisoformat(parts[1].strip())
            except ValueError:
                raise ValueError(f"Invalid stop time format: {parts[1]}")
        
        # Parse stream if provided
        if len(parts) == 3 and parts[2].strip().lower() == 'stream':
            self.stream = "active"

        # Parse periodicity if provided
        elif len(parts) == 3 and parts[2].strip():
            periodicity_match = re.match(r'(\d+(\.\d+)?)([smhd])', parts[2].strip())
            if periodicity_match:
                value, unit = periodicity_match.groups()[0], periodicity_match.groups()[2]  # Explicitly separate value and unit
                value = float(value)  # Convert value to float to handle both integers and floats
                if unit == 's':
                    self.periodicity = timedelta(seconds=value)
                elif unit == 'm':
                    self.periodicity = timedelta(minutes=value)
                elif unit == 'h':
                    self.periodicity = timedelta(hours=value)
                elif unit == 'd':
                    self.periodicity = timedelta(days=value)
            else:
                raise ValueError(f"Invalid periodicity format: {parts[2]}")
        
        return self.start, self.stop, self.periodicity, self.stream
    


class Ids:
    @staticmethod
    def calculate_capability_id(message):
        try:
            endpoint = message[MessageFields.ENDPOINT]
            capability_name = message[MessageFields.CAPABILITY_NAME]
            combined_string = Ids.combine_to_string([endpoint, capability_name])
            capability_id = hashlib.sha256(combined_string.encode()).hexdigest()
            return capability_id
        except KeyError as e:
            raise KeyError(f"Missing required field: {e}")
        except Exception as e:
            raise Exception(f"An error occurred while calculating capability ID: {e}")

    @staticmethod
    def calculate_measurement_id(message):
        try:
            capability_id = Ids.calculate_capability_id(message)
            parameters = message[MessageFields.PARAMETERS]
            schedule = message[MessageFields.SCHEDULE]
            combined_string = Ids.combine_to_string([capability_id, parameters, schedule])
            measurement_id = hashlib.sha256(combined_string.encode()).hexdigest()
            return measurement_id
        except KeyError as e:
            raise KeyError(f"Missing required field: {e}")
        except Exception as e:
            raise Exception(f"An error occurred while calculating measurement ID: {e}")

    @staticmethod
    def calculate_operation_id(message):
        try:
            measurement_id = Ids.calculate_measurement_id(message)
            nonce = message[MessageFields.NONCE]
            timestamp = message[MessageFields.TIMESTAMP]
            combined_string = Ids.combine_to_string([measurement_id, nonce, timestamp])
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
