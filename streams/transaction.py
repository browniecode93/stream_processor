import logging

import faust

import streams.settings as settings

logger = logging.getLogger(__name__)

app = faust.App(
    id=settings.TRANSACTION_CARD_APP_ID,
    debug=settings.DEBUG,
    autodiscover=False,
    origin="streams.transaction",
    broker=settings.KAFKA_BOOTSTRAP_SERVER,
    store=settings.STORE_URI,
    topic_allow_declare=settings.TOPIC_ALLOW_DECLARE,
    topic_disable_leader=settings.TOPIC_DISABLE_LEADER,
    broker_credentials=settings.SSL_CONTEXT,
    consumer_auto_offset_reset="latest",
    logging_config=settings.LOGGING
)

transaction = app.topic(settings.TOPIC_TRANSACTIONS, acks=settings.OFFSET_ACK_ON_KAFKA, partitions=None)

print("hereeeeeeeeee")

@app.agent(transaction)
async def transactions(stream):
    async for event in stream:
        print(f'>>>>>>>>>>>>>> {str(event)}')
        logger.info("Start process: " + str(event))
        # Write you code here #
