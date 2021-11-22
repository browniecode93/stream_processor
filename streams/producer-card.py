import json
import faust

import settings

import asyncio
import random
from faust.cli import option

app = faust.App('faust_producer', broker=settings.KAFKA_BOOTSTRAP_SERVER, web_port=7001)
topic = app.topic(settings.TOPIC_TRANSACTIONS)

@app.command(
    option('--max-latency',
           type=float, default=0.5, envvar='PRODUCE_LATENCY',
           help='Add delay of (at most) n seconds between publishing.')
)
async def produce(self, max_latency: float):
    print("hereee")
    """Produce example Withdrawal events."""
    with open('events/test.json', 'r') as card_events:
        data = json.load(card_events)
    for dt in data:
        print(dt)
        await topic.send(value=dt)
        if max_latency:
            await asyncio.sleep(random.uniform(0, max_latency))

if __name__ == '__main__':
    app.main()