"""Simple Faust Producer"""
import faust
import settings
import asyncio
import time
import json

if __name__ == '__main__':
    """Simple Faust Producer"""

    class Timeit(object):
        def __init__(self, msg):
            self.msg = msg
            self.start = time.time()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            print(f'{1000 * (time.time() - self.start):,.3f}ms: {self.msg}')


    data = []
    with open('events/channel_events.json', 'r') as card_events:
        data = json.load(card_events)

    # Create the Faust App
    app = faust.App('faust_producer', broker=settings.KAFKA_BOOTSTRAP_SERVER, web_port=7001)
    topic = app.topic(settings.TOPIC_TRANSACTIONS)

    @app.task
    async def testing():
        n = len(data)
        with Timeit(f"Producing {n} messages"):
            await asyncio.sleep(5)
            await asyncio.gather(*[topic.send(value=i) for i in data])

    app.main()