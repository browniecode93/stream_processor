import logging
import datetime
from dateutil import tz
import uuid

import faust

from streams.models import Card
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
card_table = app.Table('card_table', default=Card)

from_zone = tz.tzutc()
to_zone = tz.tzlocal()

@app.agent(transaction)
async def transactions(stream):
    async for event in stream:
        logger.info("Start process: " + str(event))

        request_body = event['fullDocument']['RequestBody']
        response_body = event['fullDocument']['ResponseBody']

        transaction_date = event['create_at']
        utc = datetime.datetime.fromtimestamp(transaction_date)
        utc = utc.replace(tzinfo=from_zone)
        transaction_time = utc.astimezone(to_zone)
        id=uuid.uuid1()
        card = Card(id=id,
                    transaction_time=transaction_time,
                    request_code=request_body['request_code'],
                    card_no=request_body['note']['pan'],
                    account_number=request_body['accountNumber'],
                    terminal_id=request_body['note']['terminal'],
                    terminal_type=request_body['note']['terminalType'],
                    reference_id=request_body['note']['reference_id'],
                    amount=request_body['transactionAmount'],
                    occurrence_id=request_body['occurrenceId'],
                    response_code=response_body['ResponseCode'],
                    credit_name=response_body['toCardHolderName'],
                    debit_name=response_body['fromCardHolderName'])
        card_table[id] = card
        print(f">>>>>>>>>>>> {card}")
