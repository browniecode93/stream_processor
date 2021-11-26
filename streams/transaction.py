import logging
import datetime
from dateutil import tz
import uuid
import faust
from streams.models import Card, Core
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

transaction = app.topic(settings.CARD_TOPIC_TRANSACTIONS, acks=settings.OFFSET_ACK_ON_KAFKA, partitions=None,
                        internal=True)
card_table = app.Table('card_table', default=Card)

core = app.topic(settings.CORE_TOPIC_TRANSACTIONS, acks=settings.OFFSET_ACK_ON_KAFKA, partitions=None, internal=True)
core_table = app.Table('core_table', default=Core)


def utc2local(transaction_time):
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal()
    utc = datetime.datetime.fromtimestamp(transaction_time)
    utc = utc.replace(tzinfo=from_zone)
    local_time = utc.astimezone(to_zone)
    return local_time.timestamp()


@app.agent(transaction)
async def transactions(stream):
    async for event in stream:
        logger.info("Start process: " + str(event))
        request_body = event['fullDocument']['RequestBody']
        response_body = event['fullDocument']['ResponseBody']
        transaction_date = event['fullDocument']['create_at']
        transaction_time = utc2local(transaction_date)
        id = uuid.uuid1()

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
                    debit_name=response_body['fromCardHolderName'],
                    account_id=response_body['accountId'])
        card_table[id] = card
        print(f"{repr([k for (k, v) in card_table.items()])}")


@app.agent(core)
async def core(stream):
    async for event in stream:
        logger.info("Start process: " + str(event))
        core_after = event['after']
        transaction_date = core_after['transaction_date']
        if event['op_type'] == 'I':
            id = core_after['id']
            core_obj = Core(id=id,
                            transaction_type=core_after['transaction_type_enum'],
                            account_id=core_after['account_id'],
                            occurrence_id=core_after['occurrence_id'],
                            status=core_after['status'],
                            transaction_code=core_after['transaction_code'],
                            current_balance=core_after['current_balance'],
                            transaction_date=transaction_date)
            core_table[id] = core_obj
            print(f"{repr([k for (k, v) in core_table.items()])}")

        if event['op_type'] == 'U':
            core_after = event['after']
            id_u = core_after['id']
            core_u = core_table[id]
            if core_u is not None:
                core_u.transaction_code = str(core_after['transaction_code']),
                core_u.current_balance = str(core_after['current_balance']),
                core_u.status = str(core_after['status'])
                core_table[id_u] = core_u
            print(f"{repr([k for (k, v) in core_table.items()])}")


@app.page('/core/{id}/')
@app.table_route(table=core_table, match_info='id')
async def return_core(web, request, id):
    return web.json({
        'all': [v for (k, v) in core_table.items()],
    })


@app.page('/card/{id}/')
@app.table_route(table=card_table, match_info='id')
async def return_ard(web, request, id):
    return web.json({
        'all': [v for (k, v) in card_table.items()],
    })
