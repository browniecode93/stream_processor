from datetime import datetime, timedelta
from airflow import DAG
import requests
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import sqlalchemy as sa
import psycopg2

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 8, 1),
    'retries': 10,
    'retry_delay': timedelta(minutes=60),
}
"""Default arguments used on DAG parameters."""
dag_params = {
    'dag_id': 'import_transaction',
    'default_args': default_args,
    'schedule_interval': '@daily',
    'catchup': False,
    'max_active_runs': 1,
}
"""Default parameters used on DAG instance."""


def execute_query(conn_id, query, args):
    hook = PostgresHook(postgres_conn_id=conn_id or 'airflow_db')
    conn = hook.get_conn()
    conn.autocommit = True
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    print(f'Executing \n Query: {query} \n With Parameters: {args if args else None}')
    cursor.execute(query, args if args else None)

    result = cursor.fetchall() if cursor.description else None
    cursor.close()
    conn.close()
    return result


def import_core_transactions(url, **kwargs):
    response = requests.get(url)
    values = response.json()
    for d in values['all']:
        del d['__faust']
        if isinstance(d['current_balance'], list):
            d['current_balance'] = d['current_balance'][0]
        if isinstance(d['transaction_code'], list):
            d['transaction_code'] = d['transaction_code'][0]
        d['transaction_date'] = datetime.fromtimestamp(d['transaction_date']).isoformat()
    df = pd.DataFrame(values['all'])
    pg_hook = PostgresHook(postgres_conn_id='airflow_db')
    engine = sa.create_engine(pg_hook.get_uri(), echo=False)
    execute_query(conn_id='airflow_db', query='truncate dwh.core', args='')
    df.to_sql(
        name='core',
        con=engine,
        schema='dwh',
        if_exists='append',
        index=False,
        chunksize=10000,
        method='multi',
    )
    engine.dispose()


def import_card_transactions(url, **kwargs):
    response = requests.get(url)
    values = response.json()
    for d in values['all']:
        print(f">>>>>>>>{d}")
        del d['__faust']
        d['transaction_time'] = datetime.fromtimestamp(d['transaction_time']).isoformat()
    df = pd.DataFrame(values['all'])
    pg_hook = PostgresHook(postgres_conn_id='airflow_db')
    engine = sa.create_engine(pg_hook.get_uri(), echo=False)
    execute_query(conn_id='airflow_db', query='truncate dwh.card', args='')
    df.to_sql(
        name='card',
        con=engine,
        schema='dwh',
        if_exists='append',
        index=False,
        chunksize=10000,
        method='multi',
    )
    engine.dispose()


with DAG(**dag_params) as dag:
    """Creates the DAG."""

    get_core_transactions = PythonOperator(
        task_id='import_core_transactions',
        python_callable=import_core_transactions,
        provide_context=True,
        op_kwargs={
            'url': 'http://faust:6066/core/1/',
        },
        dag=dag,
    )

    get_card_transactions = PythonOperator(
        task_id='import_card_transactions',
        python_callable=import_card_transactions,
        provide_context=True,
        op_kwargs={
            'url': 'http://faust:6066/card/1/',
        },
        dag=dag,
    )
