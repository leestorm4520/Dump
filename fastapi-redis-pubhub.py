# aioredis==2.0.0
# websockets==10.1
# fastapi==0.70.0
import asyncio
import logging
import aioredis
from aioredis.client import PubSub, Redis
from fastapi import FastAPI
from fastapi.websockets import WebSocket, WebSocketDisconnect

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
app = FastAPI()

@app.websocket('/ws')
async def ws_voting_endpoint(websocket: WebSocket):
    await websocket.accept()
    await redis_connector(websocket)


async def redis_connector(websocket: WebSocket):
    async def consumer_handler(conn: Redis, ws: WebSocket):
        try:
            while True:
                message = await ws.receive_text()
                if message:
                    await conn.publish("chat:c", message)
        except WebSocketDisconnect as exc:
            # TODO this needs handling better
            logger.error(exc)

    async def producer_handler(pubsub: PubSub, ws: WebSocket):
        await pubsub.subscribe("chat:c")
        # assert isinstance(channel, PubSub)
        try:
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True)
                if message:
                    await ws.send_text(message.get('data'))
        except Exception as exc:
            # TODO this needs handling better
            logger.error(exc)

    conn = await get_redis_pool()
    pubsub = conn.pubsub()

    consumer_task = consumer_handler(conn=conn, ws=websocket)
    producer_task = producer_handler(pubsub=pubsub, ws=websocket)
    done, pending = await asyncio.wait(
        [consumer_task, producer_task], return_when=asyncio.FIRST_COMPLETED,
    )
    logger.debug(f"Done task: {done}")
    for task in pending:
        logger.debug(f"Canceling task: {task}")
        task.cancel()


async def get_redis_pool():
    return await aioredis.from_url(f'redis://your_redis_host', encoding="utf-8", decode_responses=True)