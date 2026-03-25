import logging
import json
import asyncio
from datetime import datetime
from measurement_plane.utils.decorators import registered_capabilities
from measurement_plane.base_capability import BaseCapability
from measurement_plane.messaging.message_format import Subjects, MessageFields, Ids, TaskSchedule
from measurement_plane.protocols.NATS.nats_client import NATSClient
class Agent:
    def __init__(self, broker : str, endpoint : str):
        self.broker_url = broker
        self.endpoint = endpoint
        self.capabilities = []
        self.running = False
        self.broker_client = NATSClient(servers=[self.broker_url])
        self.running_measurements = {}
        self.measurements_queues = {}
    
    async def connect(self):
        await self.broker_client.connect()

    def load_capabilities(self):
        """
        Load and register all capabilities that have been decorated.
        """
        for capability_cls in registered_capabilities:
            capability_instance = capability_cls()
            self.register_capability(capability_instance)
    
    def register_capability(self, capability:BaseCapability):
        self.capabilities.append(capability)
        capability.set_agent(self)  # Provide capability a reference to the agent

    async def advertise_capabilities(self):
        subject = Subjects.CAPABILITIES_SUBJECT
        while self.running:
            for capability in self.capabilities:
                message = capability.construct_capability()
                await self.broker_client.publish(subject, json.dumps(message))
                logging.info(f"Advertised capability")
            await asyncio.sleep(10)

    async def start_async(self):
        await self.connect() 
        self.load_capabilities()
        if not self.capabilities:
            self.running = False
            return False
        else:
            self.running = True
            asyncio.create_task(self.advertise_capabilities())
            subject = Subjects.get_specifications_subject(self.endpoint)
            logging.info("Agent will start lesstning for events")
            await self.broker_client.subscribe(subject, self.handle_messages)
            logging.info("Agent subscribed and listening for specification messages")
            return True
    
    def start(self):
        asyncio.run(self.start_async())
        
    def stop(self):
        self.running = False
        for measurement_id, entry in list(self.running_measurements.items()):
            task = entry.get("task")
            if task:
                task.cancel()
        
    async def handle_messages(self, subject, reply, data):
        specification_msg = json.loads(data)
        logging.info("Received specification message", specification_msg)
        
        capability_id = Ids.calculate_capability_id(specification_msg)
        capability = None
        for cap in self.capabilities:
            if capability_id == Ids.calculate_capability_id(cap.construct_capability()):
                capability = cap
                break

        if capability:
            logging.info("Recived msg: {}".format(specification_msg))
            if MessageFields.SPECIFICATION in specification_msg:
                await self.send_receipt(reply, specification_msg)
                measurement_id = Ids.calculate_measurement_id(specification_msg)
                operation_id = Ids.calculate_operation_id(specification_msg)
                if measurement_id not in self.running_measurements:
                    self.measurements_queues[measurement_id] = asyncio.Queue()
                    self.running_measurements[measurement_id] = {
                            'operation_ids': [],
                            'queue': self.measurements_queues[measurement_id]
                        }
                    self.running_measurements[measurement_id]['operation_ids'].append(operation_id)
                    task = asyncio.create_task(self.process_specification_async(specification_msg, capability, measurement_id))
                    self.running_measurements[measurement_id]['task'] = task

                if operation_id not in self.running_measurements[measurement_id]['operation_ids']:
                    self.running_measurements[measurement_id]['operation_ids'].append(operation_id)

            elif MessageFields.INTERRUPT in specification_msg:
                measurement_id = Ids.calculate_measurement_id(specification_msg)
                operation_id = Ids.calculate_operation_id(specification_msg)
                if measurement_id in self.running_measurements:
                    if operation_id in self.running_measurements[measurement_id]['operation_ids']:
                        self.running_measurements[measurement_id]['operation_ids'].remove(operation_id)
                    else:
                        logging.warning(f"Specification not found.")
                    if len(self.running_measurements[measurement_id]['operation_ids']) == 0:
                        task = self.running_measurements[measurement_id]['task']
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
                        logging.info(f"Interrupt confirmed for measurement {measurement_id}")
                        await self.send_receipt(
                            reply,
                            specification_msg,
                            extra_fields={
                                MessageFields.INTERRUPT_CONFIRMED: True,
                                MessageFields.STATUS: "interrupted",
                            },
                        )
                    else:
                        await self.send_receipt(
                            reply,
                            specification_msg,
                            extra_fields={
                                MessageFields.INTERRUPT_CONFIRMED: False,
                                MessageFields.STATUS: "measurement_still_running",
                            },
                        )
                        
                else:
                    logging.warning(f"Specification not found.")
                    await self.send_receipt(
                        reply,
                        specification_msg,
                        extra_fields={
                            MessageFields.INTERRUPT_CONFIRMED: True,
                            MessageFields.STATUS: "already_stopped",
                        },
                    )
                
            else:
                logging.warning("Unknown message type.")
        else:
            logging.info("received unknown capability")
        

    async def send_receipt(self, reply, receipt_msg, extra_fields=None):
        receipt_copy = receipt_msg.copy()
        if MessageFields.SPECIFICATION in receipt_copy:
            receipt_copy[MessageFields.RECEIPT] = receipt_copy[MessageFields.SPECIFICATION]
            del receipt_copy[MessageFields.SPECIFICATION]
        elif MessageFields.INTERRUPT in receipt_copy:
            receipt_copy[MessageFields.RECEIPT] = receipt_copy[MessageFields.INTERRUPT]
        else:
            logging.warning("Recipt not supported for msg: {}".format(receipt_copy))
        receipt_copy[MessageFields.TIMESTAMP] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-4]        
        if extra_fields:
            receipt_copy.update(extra_fields)
        await self.broker_client.publish(reply, json.dumps(receipt_copy))

    async def process_specification_async(self, specification_msg, capability, measurement_id=None):
        try:
            schedule = specification_msg[MessageFields.SCHEDULE]
            task_schedule = TaskSchedule(schedule)
            parameters = specification_msg[MessageFields.PARAMETERS]

            # Wait until start time
            delay = (task_schedule.start - datetime.now()).total_seconds()
            if delay > 0:
                await asyncio.sleep(delay)

            # Streaming path
            if getattr(task_schedule, "stream", False):
                result_queue = self.measurements_queues[measurement_id]
                stream_task = await capability.async_stream(parameters, result_queue)
                self.running_measurements[measurement_id]['stream_task'] = stream_task
                while True:
                    try:
                        results = await asyncio.wait_for(result_queue.get(), timeout=3600)
                        await self.send_result_async(specification_msg, results)
                        result_queue.task_done()
                    except asyncio.TimeoutError:
                        # Check if stream is still running
                        if stream_task.done():
                            break
                    except asyncio.CancelledError:
                        raise
                # Only send EOF if the stream actually ends
                await self.send_result_async(specification_msg, MessageFields.EOF_RESULTS)

                if measurement_id in self.running_measurements:
                    del self.running_measurements[measurement_id]
                if measurement_id in self.measurements_queues: 
                    del self.measurements_queues[measurement_id]
                return

            # Single / periodic execution
            while True:
                print("Executing measurement... Once or periodically")
                results = await capability.async_task(parameters)
                if results:
                    await self.send_result_async(specification_msg, results)

                if not task_schedule.periodicity:
                    break

                await asyncio.sleep(task_schedule.periodicity.total_seconds())

            await self.send_result_async(specification_msg, MessageFields.EOF_RESULTS)

            if measurement_id in self.running_measurements:
                del self.running_measurements[measurement_id]
            if measurement_id in self.measurements_queues: 
                del self.measurements_queues[measurement_id]
            return
        
        except asyncio.CancelledError:
            logging.info(f"Measurement task cancelled for {measurement_id}")
            if (measurement_id in self.running_measurements and 
                'stream_task' in self.running_measurements[measurement_id]):
                stream_task = self.running_measurements[measurement_id]['stream_task']
                if stream_task and not stream_task.done():
                    logging.info(f"Cancelling stream task for measurement {measurement_id}")
                    stream_task.cancel()
                    try:
                        await stream_task
                    except asyncio.CancelledError:
                        logging.info(f"Stream task cancelled for measurement {measurement_id}")
            
            await self.send_result_async(specification_msg, MessageFields.EOF_RESULTS)
            
            if measurement_id in self.running_measurements:
                del self.running_measurements[measurement_id]
            if measurement_id in self.measurements_queues:
                del self.measurements_queues[measurement_id]
            return

    async def send_result_async(self, specification_msg, results):
        try:
            result_msg = specification_msg.copy()
            measurement_id = Ids.calculate_measurement_id(result_msg)
            result_msg[MessageFields.RESULT] = result_msg[MessageFields.SPECIFICATION]
            result_msg[MessageFields.TIMESTAMP] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-4]
            del result_msg[MessageFields.SPECIFICATION]
            resultValues= []
            resultValues.append(results)
            result_msg[MessageFields.RESULT_VALUES] = resultValues
            result_subject = Subjects.get_results_subject(str(measurement_id))
            await self.broker_client.publish(result_subject, json.dumps(result_msg))
        except Exception as e:
            logging.error(f"[SEND] Error sending result: {e}", exc_info=True)  
