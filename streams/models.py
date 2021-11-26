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
    account_id: str = None


class Core(faust.Record):
    id: str = None
    transaction_type: str = None
    account_id: str = None
    occurrence_id: str = None
    status: str = None
    transaction_code: str = None
    current_balance: int = None
    transaction_date: str = None


class CARD_CORE(faust.Record):
    card_transaction_time: str = None
    card_request_code: int = None
    card_card_no: str = None
    card_account_number: str = None
    card_terminal_id: str = None
    card_terminal_type: str = None
    card_reference_id: str = None
    card_amount: str = None
    card_occurrence_id: str = None
    card_response_code: str = None
    card_credit_name: str = None
    card_debit_name: str = None
    core_id: str = None
    core_transaction_type: str = None
    core_account_id: str = None
    core_occurrence_id: str = None
    core_status: str = None
    core_transaction_code: str = None
    core_current_balance: str = None
    core_transaction_date: str = None
