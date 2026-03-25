import json, pickle, uuid
import logging
from datetime import datetime
from jsonschema import validate, exceptions as jsonschema_exceptions
from measurement_plane.protocols.NATS.nats_client import NATSClient
from measurement_plane.messaging.message import Message
from measurement_plane.measurement_plane_client.utils.capability_register import CapabilityRegister
import asyncio
from measurement_plane.messaging.message_format import MessageFields
class MeasurementPlaneClient:
    def __init__(self, broker_url) -> None:
        self.broker_url = broker_url
        self.broker_client = NATSClient(servers=[broker_url])
        self.capability_register = None
        self.capabilityRegister = None
    
    async def connect(self):
        await self.broker_client.connect()
        self.capability_register = CapabilityRegister(broker_client=self.broker_client)
        self.capabilityRegister = self.capability_register
        await self.capability_register.start()
    
    async def close(self):
        await self.broker_client.close()

    def get_capabilities(self, capability_types=None):
        caps = self.capability_register.capability_manager.capabilities.copy()
        if capability_types:
            return {k: v for k, v in caps.items() if v[MessageFields.CAPABILITY] in capability_types}
        return caps

    def combine_to_string(self, attributes: list) -> str:
        return ''.join(str(att).replace(" ", "").replace("\n", "") for att in attributes)
    
    def calculate_capability_id(self, message):
        return Message.calculate_capability_id(message=message)

    def create_measurement(self, capability: dict) -> 'Measurement':
        return Measurement(capability, self)

    async def send_measurement(self, measurement: 'Measurement'):
        print("Sending measurement...")
        if not measurement.valid:
            logging.error("Measurement not valid for sending")
            return
        spec_endpoint = measurement.specification_message["endpoint"]
        specification_subject = f'{spec_endpoint}.specifications'
        measurement_id = Message.calculate_measurement_id(message = measurement.specification_message)
        subject = f'{measurement_id}.results'
        measurement.result_subscription = await self.broker_client.subscribe(subject=subject,
                                                                    callback=measurement._result_handler)
        try:
            await self.broker_client.send_message_with_reply_to(
                subject=specification_subject,
                message=json.dumps(measurement.specification_message),
                receipt_receiver_on_message_callback=measurement.receipt_receiver_on_message_callback,
                timeout=5
            )
            measurement.active = True
            logging.info("Measurement sent")
        except asyncio.TimeoutError:
            logging.error("Specification receipt timeout — stopping measurement.")
            await measurement.stop()
        

    async def interrupt_measurement(self, measurement: 'Measurement'):
        return await measurement.interrupt()

class Measurement:
    def __init__(self, capability: dict, measurement_plane_client: MeasurementPlaneClient):
        self.measurement_plane_client = measurement_plane_client
        self.broker_url = self.measurement_plane_client.broker_url
        self.capability = capability
        self.results = []
        self.config = {}
        self.result_subscription = None
        self.specification_message = capability.copy()
        self.specification_message[MessageFields.SPECIFICATION] = self.specification_message.pop(MessageFields.CAPABILITY)
        self.valid = False
        self.active = False
        self.stop_confirmed = False
        self.last_interrupt_status = None

    def configure(self, schedule: str, parameters: dict, result_callback, stream_results: bool = False, redirect_to_storage: bool = False, completion_callback = None) -> bool:
        if self.validate_parameters(parameters):
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-4]
            nonce = uuid.uuid4().hex
            if stream_results: schedule += "| stream"
            self.specification_message.update({
                MessageFields.PARAMETERS: parameters,
                MessageFields.SCHEDULE: schedule,
                MessageFields.TIMESTAMP: timestamp,
                MessageFields.NONCE: nonce
            })
            self.config = {
                "stream_results": stream_results,
                "redirect_to_storage": redirect_to_storage,
                "result_callback": result_callback,
                "completion_callback": completion_callback,
            }
            self.valid = True
        else:
            self.valid= False

    async def receipt_receiver_on_message_callback(self, subject, reply, data):
        receipt_msg = json.loads(data)
        if MessageFields.RECEIPT in receipt_msg:
            if  MessageFields.INTERRUPT in receipt_msg:
                logging.info("Measurement interrupted.")                

    async def _result_handler(self, subject, reply, data):

        # Convert memoryview to bytes if necessary
        if isinstance(data, memoryview):
            data = data.tobytes()

        # Try to decode as JSON or fall back to pickle
        result_msg = None
        if isinstance(data, bytes):
            # If it's bytes, assume it's pickled binary data
            try:
                result_msg = pickle.loads(data)
            except pickle.UnpicklingError as e:
                logging.error(f"Failed to deserialize message with pickle: {e}")
                result_msg = None
        else:
            try:
                result_msg = json.loads(data)
            except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as e:
                logging.error(f"Failed to decode message as JSON: {e}")
                result_msg = None    

        # Proceed if decoding was successful
        if result_msg and MessageFields.RESULT in result_msg:
            results = result_msg[MessageFields.RESULT_VALUES]
            if MessageFields.EOF_RESULTS in results:
                logging.info("End of results received.")
                await self.stop()
                return
            logging.info(f"Results from {subject}")
            self.config['result_callback'](results)

    async def stop(self):
        if self.result_subscription:
            await self.measurement_plane_client.broker_client.unsubscribe(self.result_subscription)
            self.result_subscription = None
        self.active = False
        completion_callback = self.config.get("completion_callback")
        if completion_callback:
            try:
                completion_callback()
            except Exception as e:
                logging.error(f"Error in completion callback: {e}", exc_info=True)

            
    async def interrupt(self):
        try:
            self.stop_confirmed = False
            self.last_interrupt_status = None
            interrupt_msg = self.specification_message.copy() 
            
            # Transform specification -> interrupt
            interrupt_msg[MessageFields.INTERRUPT] = interrupt_msg[MessageFields.SPECIFICATION]
            del interrupt_msg[MessageFields.SPECIFICATION]
            
            # Send interrupt message
            spec_endpoint = interrupt_msg["endpoint"]
            specification_subject = f'{spec_endpoint}.specifications'
            
            logging.info(f"Sending interrupt message: {interrupt_msg}")
            
            await self.measurement_plane_client.broker_client.send_message_with_reply_to(
                subject=specification_subject,
                message=json.dumps(interrupt_msg),
                receipt_receiver_on_message_callback=self.interrupt_receipt_callback,
                timeout=10
            )
            if self.stop_confirmed:
                await self.stop()
                logging.info("Interrupt confirmed by agent")
            else:
                logging.warning("Interrupt reply received without stop confirmation")
            return self.stop_confirmed
        except Exception as e:
            logging.error(f"Error sending interrupt: {e}", exc_info=True)
            return False
    
    async def interrupt_receipt_callback(self, subject, reply, data):
        """Handle interrupt receipt"""
        try:
            if not data or len(data) == 0:
                logging.warning("Received empty interrupt receipt")
                return
                
            receipt_msg = json.loads(data)
            logging.info(f"Interrupt receipt received: {receipt_msg}")
            
            if MessageFields.RECEIPT in receipt_msg:
                self.stop_confirmed = bool(receipt_msg.get(MessageFields.INTERRUPT_CONFIRMED, False))
                self.last_interrupt_status = receipt_msg.get(MessageFields.STATUS)
                logging.info(
                    "Interrupt receipt status=%s confirmed=%s",
                    self.last_interrupt_status,
                    self.stop_confirmed,
                )
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode interrupt receipt: {e}, data: {data}")
        except Exception as e:
            logging.error(f"Error in interrupt receipt callback: {e}", exc_info=True)

    def validate_parameters(self, parameters: dict) -> bool:
        try:
            validate(instance=parameters, schema=self.capability[MessageFields.PARAMETERS_SCHEMA])
            return True
        except jsonschema_exceptions.ValidationError as err:
            logging.error(f"Validation error: {err.message}")
            return False
