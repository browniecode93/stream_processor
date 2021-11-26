# stream_processor
process real time card and core transactions and populate it to postgres via airflow and create report over it.

## Built With

* Faust
* Postgresql
* Airflow

<!-- GETTING STARTED -->
## Getting Started

Clone the repo in your local machine.

### Prerequisites

* Install Docker Desktop

<!-- USAGE EXAMPLE -->
## Usage

- run docker-compose up


populate message:
In another tabs
- cd streams and run `python3 producer-core.py produce` to produce core transaction
- cd streams and run `python3 producer-card.py produce` to produce card transaction

When all transactions produced successfuly, use http://0.0.0.0:8080/admin to connect to airflow UI

From Admin->Connections, edit `airflow_db` with below configuration:
conn_type: postgres
host: db
schema: transaction
user and password: admin

Turn on `import_transaction` to import data.

Once its finished you can turn on any other dags to create metric tables.

You can check created tables by connecting to db container with `docker-compose exec db sh` and `psql -h db transaction admin`.

All tables are availabale on `dwh` schema.

Sample:

![transaction_amount_per_id](https://pasteboard.co/0QqqCWxzKMkf.png)
