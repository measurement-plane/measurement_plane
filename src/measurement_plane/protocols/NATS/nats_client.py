import logging
from nats.aio.client import Client as NATS
from nats.js.api import StreamConfig, RetentionPolicy
from nats.aio.errors import ErrTimeout
import uuid, asyncio

logger = logging.getLogger("NATSClient")
logger.setLevel(logging.INFO)

class NATSClient:
    def __init__(self, servers=["nats://127.0.0.1:4222"]):
        self.servers = servers
        self.nc = NATS()
        self.js = None  # JetStream context
        self.subscriptions = {}

    async def connect(self):
        if not self.nc.is_connected:
            await self.nc.connect(servers=self.servers,
                                  reconnect =True,
                                  max_reconnect_attempts = -1,
                                  reconnect_time_wait = 2)
            logger.info(f"Connected to NATS servers: {self.servers}")
            self.js = self.nc.jetstream()

    async def close(self):
        if self.nc.is_connected:
            await self.nc.flush()
            await self.nc.close()
            logger.info("Connection closed.")

    # ---------------------------
    # Core Publish / Subscribe
    # ---------------------------
    async def publish(self, subject: str, message: str):
        if not self.nc.is_connected:
            await self.connect()
        await self.nc.publish(subject, message.encode())

    async def subscribe(self, subject: str, callback):
        if not self.nc.is_connected:
            await self.connect()

        async def handler(msg):
            data = msg.data.decode()
            await callback(msg.subject, msg.reply, data)

        sid = await self.nc.subscribe(subject, cb=handler)
        self.subscriptions[sid] = subject
        return sid

    async def unsubscribe(self, sid):
        if sid in self.subscriptions:
            try:
                await self.nc.unsubscribe(sid)
            except Exception:
                pass
            finally:
                self.subscriptions.pop(sid, None)

    async def request(self, subject: str, message: str, timeout=1):
        if not self.nc.is_connected:
            await self.connect()
        try:
            msg = await self.nc.request(subject, message.encode(), timeout=timeout)
            return msg.data.decode()
        except ErrTimeout:
            return None

    # --------------------------- send_spec_with_reply
    async def send_message_with_reply_to(self, subject, message: str, receipt_receiver_on_message_callback, timeout=5):
        """
        - Create temporary random subject
        - Subscribe to it
        - Send message with reply-to
        - Wait for reply (timeout: 5s)
        """

        if not self.nc.is_connected:
            await self.connect()

        reply_subject = f"_INBOX.{uuid.uuid4().hex}"
        
        loop = asyncio.get_event_loop()
        future = loop.create_future()

        async def handler(subject, reply, data):
            if not future.done():
                future.set_result((subject, data))
                await receipt_receiver_on_message_callback(subject, data)

        sid = await self.subscribe(reply_subject, handler)

        await self.nc.publish(subject, message.encode(), reply=reply_subject)

        try:
            await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(f"No reply received after {timeout}s.")
        finally:
            await self.unsubscribe(sid)

            
    # ---------------------------
    # JetStream Streaming
    # ---------------------------
    async def ensure_stream(self, stream: str, subject: str):
        """Create stream only once"""
        try:
            await self.js.stream_info(stream)
        except:
            await self.js.add_stream(
                StreamConfig(
                    name=stream,
                    subjects=[subject],
                    retention=RetentionPolicy.INTEREST,
                    max_age=1_000_000_000,
                )
            )

    async def stream_publish(self, stream: str, subject: str, data: bytes):
        """Publish large binary/JSON streaming content (no manual chunking)"""
        if not self.nc.is_connected:
            await self.connect()

        await self.ensure_stream(stream, subject)
        await self.js.publish(subject, data)

    async def stream_subscribe(self, stream: str, subject: str, callback):
        """Subscribe to JetStream stream with backpressure"""
        if not self.nc.is_connected:
            await self.connect()

        await self.ensure_stream(stream, subject)

        async def handler(msg):
            await callback(msg.data)
            await msg.ack()

        sub = await self.js.subscribe(subject, cb=handler)
        self.subscriptions[sub._sid] = subject
        return sub._sid
