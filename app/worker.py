import asyncio
import json
import os
import aio_pika
from dotenv import load_dotenv

load_dotenv()
RABBIT_URL = os.getenv("RABBIT_URL")

async def main():
    connection = await aio_pika.connect_robust(RABBIT_URL)
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        queue = await channel.declare_queue("orders_queue", durable=True)
        print("Waiting for orders...")
        # Listen for messages arriving in the queue
        async with queue.iterator() as messages:
            # Wait asynchronously for the next message
            async for message in messages:
                # Process the message; ACK when successful
                async with message.process():
                    order = json.loads(message.body.decode())
                    method = order.get("delivery_method", "unknown")
                    print(f"Received order {order['order_id']} ({method})")
                    await asyncio.sleep(12)
                    print(f"Processed: {order['quantity']} x {order['product']} ({method})")

if __name__ == "__main__":
    asyncio.run(main())
