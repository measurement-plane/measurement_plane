import asyncio
import json
import unittest

from measurement_plane.agent import Agent
from measurement_plane.measurement_plane_client.mp_client import Measurement
from measurement_plane.messaging.message_format import (
    ExecutionModes,
    LifecycleStates,
    MessageFields,
    MessageTypes,
    TaskSchedule,
)


class DummyClient:
    broker_url = "nats://example.invalid:4222"


class RecordingBroker:
    def __init__(self):
        self.messages = []

    async def publish(self, subject, message):
        self.messages.append((subject, json.loads(message)))


class StatusAndScheduleTests(unittest.TestCase):
    def test_stream_execution_mode_does_not_corrupt_schedule(self):
        capability = {
            MessageFields.CAPABILITY: "measure-count-rate",
            MessageFields.ENDPOINT: "/timetagger/alice",
            MessageFields.CAPABILITY_NAME: "count_rate_measurement",
            MessageFields.PARAMETERS_SCHEMA: {"type": "object"},
            MessageFields.METADATA: {},
        }
        measurement = Measurement(capability, DummyClient())

        measurement.configure("now", {}, lambda _: None, stream_results=True)

        self.assertEqual(measurement.specification_message[MessageFields.SCHEDULE], "now")
        self.assertEqual(
            measurement.specification_message[MessageFields.EXECUTION_MODE],
            ExecutionModes.INFINITE_STREAM,
        )

    def test_legacy_and_canonical_stream_schedules_are_parsed(self):
        self.assertEqual(TaskSchedule("now| stream").stream, "active")
        self.assertEqual(TaskSchedule("now||stream").stream, "active")

    def test_status_message_includes_reason_and_source(self):
        agent = Agent("nats://example.invalid:4222", "/timetagger/alice")
        broker = RecordingBroker()
        agent.broker_client = broker
        specification = {
            MessageFields.ENDPOINT: "/timetagger/alice",
            MessageFields.CAPABILITY_NAME: "count_rate_measurement",
            MessageFields.EXECUTION_MODE: ExecutionModes.INFINITE_STREAM,
        }

        asyncio.run(agent.send_lifecycle_event(
            specification,
            "measurement-1",
            "measurement_failed",
            LifecycleStates.FAILED,
            {
                MessageFields.ERROR: "device disconnected",
                MessageFields.ERROR_TYPE: "RuntimeError",
            },
        ))

        _, message = broker.messages[0]
        self.assertEqual(message[MessageFields.MESSAGE_TYPE], MessageTypes.MEASUREMENT_STATUS)
        self.assertEqual(message[MessageFields.STATUS], LifecycleStates.FAILED)
        self.assertEqual(message[MessageFields.ERROR], "device disconnected")
        self.assertEqual(message[MessageFields.ERROR_TYPE], "RuntimeError")
        self.assertEqual(message[MessageFields.SOURCE][MessageFields.ENDPOINT], "/timetagger/alice")


if __name__ == "__main__":
    unittest.main()
