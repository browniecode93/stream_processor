CREATE SCHEMA dwh

CREATE TABLE IF NOT EXISTS dwh.card(
    id uuid NOT NULL,
    transaction_time timestamp with timezone,
    request_code int,
    card_no varchar(80),
    account_number varchar(80),
    terminal_id varchar(80),
    terminal_type int,
    reference_id varchar(50),
    amount bigint,
    occurrence_id varchar(100),
    response_code varchar(10),
    credit_name varchar(50),
    debit_name varchar(50),
    account_id int
)

CREATE TABLE IF NOT EXISTS dwh.core(
    id uuid NOT NULL,
    transaction_type int,
    account_id int,
    occurrence_id varchar(100),
    status varchar(5),
    transaction_code varchar(50),
    current_balance bigint,
    transaction_date timestamp with timezone
)
