import logging
import datetime
from dateutil import tz
import uuid
import faust

from streams.models import Card, Core
import streams.settings as settings
import datetime


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

transaction = app.topic(settings.TOPIC_TRANSACTIONS, acks=settings.OFFSET_ACK_ON_KAFKA, partitions=8)
card_table = app.Table('card_table', default=Card)

core = app.topic('core_transaction', acks=settings.OFFSET_ACK_ON_KAFKA, partitions=8)
core_table = app.Table('core_table', default=Core)

from_zone = tz.tzutc()
to_zone = tz.tzlocal()


@app.agent(transaction)
async def transactions(stream):
    async for event in stream:
        logger.info("Start process: " + str(event))
        request_body = event['fullDocument']['RequestBody']
        response_body = event['fullDocument']['ResponseBody']

        transaction_date = event['fullDocument']['create_at']
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
        print(f"{repr([k for (k,v) in card_table.items()])}")


@app.agent(core)
async def core(stream):
    async for event in stream:
        logger.info("Start process: " + str(event))
        core_after = event['after']
        if event['op_type'] == 'I':
            transaction_date = core_after['transaction_date']
            utc = datetime.datetime.fromtimestamp(transaction_date)
            utc = utc.replace(tzinfo=from_zone)
            transaction_time = utc.astimezone(to_zone)
            id = core_after['id']
            core_obj = Core(id=id,
                        transaction_type=core_after['transaction_type_enum'],
                        account_id=core_after['account_id'],
                        occurrence_id=core_after['occurrence_id'],
                        status=core_after['status'],
                        transaction_code=core_after['transaction_code'],
                        current_balance=core_after['current_balance'],
                        transaction_date=transaction_time)
            core_table[id] = core_obj
            print(f"{repr([k for (k, v) in core_table.items()])}")

        if event['op_type'] == 'U':
            core_after = event['after']
            id_u = core_after['id']
            list_of_values = core_table[id_u].value()
            list_of_values['transaction_code'] = core_after['transaction_code'],
            list_of_values['current_balance'] = core_after['current_balance'],
            list_of_values['status'] = core_after['status']
            core_table[id_u] = list_of_values
            print(f"{repr([k for (k, v) in core_table.items()])}")



