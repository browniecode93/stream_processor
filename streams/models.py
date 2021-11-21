import faust

class Card(faust.Record):
    request_code: int = None
    card_no: str = None
    account_number: str = None
    terminal_id: str = None
    terminal_type: str = None
    reference_id: str = None
    amount: str = None
    occurrence_id: str = None