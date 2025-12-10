from src.measurement_plane.protocols.NATS.nats_client import NATSClient
import asyncio

async def main():
    client = NATSClient(["nats://localhost:4222"])

    async def on_message(subject, reply, data):
        print(f"[RECEIVED] {subject} -> {data}")
        # If someone expects reply
        if reply:
            await client.publish(reply, f"ACK: {data}")

    # Connect
    await client.connect()

    # Subscribe
    await client.subscribe("demo.updates", on_message)

    # Publish
    await client.publish("demo.updates", "Hello From NATS")

    # Request/Reply demo
    response = await client.request("api.fetch", "Give me status")
    print("RPC Response:", response)

    # Keep running to receive messages
    await asyncio.sleep(10)

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())