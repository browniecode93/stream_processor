import faust

class Card(faust.Record):
    id: str = None
    transaction_time: str = None
    request_code: int = None
    card_no: str = None
    account_number: str = None
    terminal_id: str = None
    terminal_type: str = None
    reference_id: str = None
    amount: str = None
    occurrence_id: str = None
    response_code: str = None
    credit_name: str = None
    debit_name: str = None


class Core(faust.Record):
    id: str = None
    transaction_type: str = None
    account_id: str = None
    occurrence_id: str = None
    status: str = None
    transaction_code: str = None
    current_balance: str = None
    transaction_date: str = None