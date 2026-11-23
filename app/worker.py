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
        async with queue.iterator() as messages: 
            async for message in messages: 
                async with message.process(): 
                    order = json.loads(message.body.decode()) 
                    print(f"Received order {order['order_id']}") 
                    await asyncio.sleep(12) 
                    print(f"Processed: {order['quantity']} x {order['product']}") 

if __name__ == "__main__": 
    asyncio.run(main()) 