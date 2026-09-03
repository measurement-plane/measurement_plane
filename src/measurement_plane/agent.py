import logging
import json
import asyncio
from datetime import datetime
from measurement_plane.utils.decorators import registered_capabilities
from measurement_plane.base_capability import BaseCapability
from measurement_plane.messaging.message_format import (
    Subjects,
    MessageFields,
    Ids,
    TaskSchedule,
    ExecutionModes,
    LifecycleStates,
    MessageTypes,
)
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
        
        capability = None
        for cap in self.capabilities:
            if cap.matches_specification(specification_msg):
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
                            'queue': self.measurements_queues[measurement_id],
                            'event_sequence': 0,
                        }
                    self.running_measurements[measurement_id]['operation_ids'].append(operation_id)
                    task = asyncio.create_task(self.process_specification_async(specification_msg, capability, measurement_id))
                    self.running_measurements[measurement_id]['task'] = task
                    await self.send_lifecycle_event(
                        specification_msg,
                        measurement_id,
                        event_name="measurement_accepted",
                        state=LifecycleStates.ACCEPTED,
                        payload={
                            MessageFields.STATUS: "accepted",
                            MessageFields.EXECUTION_MODE: self._resolve_execution_mode(specification_msg),
                        },
                    )

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
            execution_mode = self._resolve_execution_mode(specification_msg, task_schedule)
            result_queue = self.measurements_queues[measurement_id]

            async def lifecycle_emitter(event_name, state, payload=None):
                await self.send_lifecycle_event(
                    specification_msg,
                    measurement_id,
                    event_name=event_name,
                    state=state,
                    payload=payload or {},
                )

            capability.bind_lifecycle_emitter(lifecycle_emitter)

            # Wait until start time
            delay = (task_schedule.start - datetime.now()).total_seconds()
            if delay > 0:
                await asyncio.sleep(delay)

            await self.send_lifecycle_event(
                specification_msg,
                measurement_id,
                event_name="measurement_started",
                state=LifecycleStates.RUNNING,
                payload={MessageFields.EXECUTION_MODE: execution_mode},
            )

            if execution_mode in {ExecutionModes.FINITE_STREAM, ExecutionModes.INFINITE_STREAM}:
                stream_task = await capability.async_stream(parameters, result_queue)
                self.running_measurements[measurement_id]['stream_task'] = stream_task
                while True:
                    try:
                        if task_schedule.stop and datetime.now() >= task_schedule.stop:
                            if stream_task and not stream_task.done():
                                stream_task.cancel()
                                try:
                                    await stream_task
                                except asyncio.CancelledError:
                                    pass
                            break
                        results = await asyncio.wait_for(result_queue.get(), timeout=1)
                        await self.send_result_async(specification_msg, results)
                        result_queue.task_done()
                        if stream_task.done() and result_queue.empty():
                            stream_task.result()
                            break
                    except asyncio.TimeoutError:
                        if stream_task.done():
                            stream_task.result()
                            break
                    except asyncio.CancelledError:
                        raise
                await self.send_result_async(specification_msg, MessageFields.EOF_RESULTS)
                await self.send_lifecycle_event(
                    specification_msg,
                    measurement_id,
                    event_name="measurement_completed",
                    state=LifecycleStates.COMPLETED,
                    payload={MessageFields.STATUS: "completed"},
                )

                self._clear_measurement(measurement_id)
                return

            while True:
                results = await capability.async_task(parameters)
                if results:
                    await self.send_result_async(specification_msg, results)

                if not task_schedule.periodicity:
                    break

                await asyncio.sleep(task_schedule.periodicity.total_seconds())

            await self.send_result_async(specification_msg, MessageFields.EOF_RESULTS)
            await self.send_lifecycle_event(
                specification_msg,
                measurement_id,
                event_name="measurement_completed",
                state=LifecycleStates.COMPLETED,
                payload={MessageFields.STATUS: "completed"},
            )
            self._clear_measurement(measurement_id)
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
            await self.send_lifecycle_event(
                specification_msg,
                measurement_id,
                event_name="measurement_interrupted",
                state=LifecycleStates.INTERRUPTED,
                payload={MessageFields.STATUS: "interrupted"},
            )
            self._clear_measurement(measurement_id)
            return
        except Exception as e:
            logging.error(f"Measurement task failed for {measurement_id}: {e}", exc_info=True)
            await self.send_lifecycle_event(
                specification_msg,
                measurement_id,
                event_name="measurement_failed",
                state=LifecycleStates.FAILED,
                payload={
                    MessageFields.STATUS: "failed",
                    MessageFields.ERROR: str(e),
                    MessageFields.ERROR_TYPE: type(e).__name__,
                },
            )
            await self.send_result_async(specification_msg, MessageFields.EOF_RESULTS)
            self._clear_measurement(measurement_id)
            return

    async def send_result_async(self, specification_msg, results):
        try:
            result_msg = specification_msg.copy()
            measurement_id = Ids.calculate_measurement_id(result_msg)
            result_msg[MessageFields.RESULT] = result_msg[MessageFields.SPECIFICATION]
            result_msg[MessageFields.TIMESTAMP] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-4]
            result_msg[MessageFields.MEASUREMENT_ID] = str(measurement_id)
            result_msg[MessageFields.PLANE] = MessageFields.DATA_PLANE
            del result_msg[MessageFields.SPECIFICATION]
            resultValues= []
            resultValues.append(results)
            result_msg[MessageFields.RESULT_VALUES] = resultValues
            result_subject = Subjects.get_results_subject(str(measurement_id))
            await self.broker_client.publish(result_subject, json.dumps(result_msg))
        except Exception as e:
            logging.error(f"[SEND] Error sending result: {e}", exc_info=True)  

    async def send_lifecycle_event(self, specification_msg, measurement_id, event_name, state, payload=None):
        try:
            payload = payload or {}
            entry = self.running_measurements.get(measurement_id)
            sequence = 1
            if entry is not None:
                sequence = entry.get("event_sequence", 0) + 1
                entry["event_sequence"] = sequence
            status_messages = {
                LifecycleStates.ACCEPTED: "Measurement accepted by the capability",
                LifecycleStates.RUNNING: "Measurement is running",
                LifecycleStates.RETRYING: "Measurement is retrying",
                LifecycleStates.COMPLETED: "Measurement completed successfully",
                LifecycleStates.INTERRUPTED: "Measurement was interrupted",
                LifecycleStates.FAILED: "Measurement failed",
            }
            error = payload.get(MessageFields.ERROR)
            status_message = payload.get(MessageFields.STATUS_MESSAGE) or (
                f"Measurement failed: {error}" if state == LifecycleStates.FAILED and error
                else status_messages.get(state, event_name.replace("_", " ").capitalize())
            )
            try:
                execution_mode = self._resolve_execution_mode(specification_msg)
            except (KeyError, TypeError, ValueError):
                execution_mode = specification_msg.get(MessageFields.EXECUTION_MODE)
            event_msg = {
                MessageFields.MESSAGE_TYPE: MessageTypes.MEASUREMENT_STATUS,
                MessageFields.MEASUREMENT_ID: str(measurement_id),
                MessageFields.ENDPOINT: specification_msg.get(MessageFields.ENDPOINT),
                MessageFields.CAPABILITY_NAME: specification_msg.get(MessageFields.CAPABILITY_NAME),
                MessageFields.TIMESTAMP: datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-4],
                MessageFields.PLANE: MessageFields.CONTROL_PLANE,
                MessageFields.LIFECYCLE_EVENT: event_name,
                MessageFields.LIFECYCLE_STATE: state,
                MessageFields.STATUS: state,
                MessageFields.STATUS_MESSAGE: status_message,
                MessageFields.SEQUENCE: sequence,
                MessageFields.EVENT_PAYLOAD: payload,
                MessageFields.EXECUTION_MODE: execution_mode,
                MessageFields.SOURCE: {
                    "kind": "capability",
                    MessageFields.ENDPOINT: specification_msg.get(MessageFields.ENDPOINT),
                    MessageFields.CAPABILITY_NAME: specification_msg.get(MessageFields.CAPABILITY_NAME),
                },
            }
            if error:
                event_msg[MessageFields.ERROR] = error
                event_msg[MessageFields.ERROR_TYPE] = payload.get(MessageFields.ERROR_TYPE, "RuntimeError")
            await self.broker_client.publish(
                Subjects.get_events_subject(str(measurement_id)),
                json.dumps(event_msg),
            )
        except Exception as e:
            logging.error(f"[EVENT] Error sending lifecycle event: {e}", exc_info=True)

    def _resolve_execution_mode(self, specification_msg, task_schedule=None):
        explicit = specification_msg.get(MessageFields.EXECUTION_MODE)
        if explicit:
            return explicit
        if task_schedule is None:
            schedule = specification_msg.get(MessageFields.SCHEDULE, "")
            task_schedule = TaskSchedule(schedule)
        if getattr(task_schedule, "stream", False):
            return ExecutionModes.INFINITE_STREAM
        return ExecutionModes.ONE_SHOT

    def _clear_measurement(self, measurement_id):
        if measurement_id in self.running_measurements:
            del self.running_measurements[measurement_id]
        if measurement_id in self.measurements_queues:
            del self.measurements_queues[measurement_id]
