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
    'start_date': datetime(2021, 11, 17),
    'retries': 10,
    'retry_delay': timedelta(minutes=60),
}
"""Default arguments used on DAG parameters."""
dag_params = {
    'dag_id': 'create_amount_report',
    'default_args': default_args,
    'schedule_interval': '@daily',
    'catchup': True,
    'max_active_runs': 1,
}
"""Default parameters used on DAG instance."""


def execute_query(conn_id, query, args, **kwargs):
    hook = PostgresHook(postgres_conn_id=conn_id or 'airflow_db')
    conn = hook.get_conn()
    conn.autocommit = True
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    print(f'Executing \n Query: {query} \n With Parameters: {args if args else None}')
    cursor.execute(query, args if args else None)

    result = cursor.fetchall() if cursor.description else None
    cursor.close()
    conn.close()
    kwargs['task_instance'].xcom_push(key='query_result', value=result)


def insert_into_account_amount_table(**kwargs):
    result = kwargs['task_instance'].xcom_pull(task_ids='get_transaction_count_and_amount', key='query_result')
    df = pd.DataFrame(result)
    df.transpose()
    df.columns = ['account_id', 'transaction_type', 'transaction_count', 'transaction_amount', 'transaction_date']
    pg_hook = PostgresHook(postgres_conn_id='airflow_db')
    engine = sa.create_engine(pg_hook.get_uri(), echo=False)
    df.to_sql(
        name='amount_report',
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

    execute_amount_account_query = PythonOperator(
        task_id='get_transaction_count_and_amount',
        provide_context=True,
        python_callable=execute_query,
        op_kwargs={
            'conn_id': 'airflow_db',
            'query': 'select dca.account_id, dco.transaction_type, count(dca.id) as transaction_count, sum(dca.amount) as transaction_amount, dca.transaction_time::date from dwh.card dca join dwh.core dco on dca.account_id = dco.account_id where dca.transaction_time::date  = %(date)s::DATE group by dca.account_id, dco.transaction_type, dca.transaction_time::date',
            'args': {'date': '2021-11-17'},
        },
        dag=dag,
    )

    insert_into_account_amount_table = PythonOperator(
        task_id='insert_into_account_amount_table',
        provide_context=True,
        python_callable=insert_into_account_amount_table,
        dag=dag,
    )

    execute_amount_account_query >> insert_into_account_amount_table