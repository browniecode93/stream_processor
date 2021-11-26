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
    'dag_id': 'calculate_end_of_day_balance',
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


def insert_into_eod_table(**kwargs):
    result = kwargs['task_instance'].xcom_pull(task_ids='execute_eod_query', key='query_result')
    if result:
        df = pd.DataFrame(result)
        df.transpose()
        df.columns = ['transaction_date', 'account_id', 'current_balance']
        pg_hook = PostgresHook(postgres_conn_id='airflow_db')
        engine = sa.create_engine(pg_hook.get_uri(), echo=False)
        df.to_sql(
            name='end_of_day_balance',
            con=engine,
            schema='dwh',
            if_exists='append',
            index=False,
            chunksize=10000,
            method='multi',
        )
        engine.dispose()


query = '''
SELECT transaction_date, account_id, current_balance
FROM (
  SELECT transaction_date, account_id, current_balance,
         ROW_NUMBER() OVER (PARTITION BY account_id, transaction_date::date
                            ORDER BY transaction_date DESC) AS rn
  FROM dwh.core
  WHERE transaction_date::date = %(date)s::DATE - INTERVAL '1 DAY') AS t
WHERE t.rn = 1
;
'''

with DAG(**dag_params) as dag:
    """Creates the DAG."""

    execute_eod_query = PythonOperator(
        task_id='execute_eod_query',
        provide_context=True,
        python_callable=execute_query,
        op_kwargs={
            'conn_id': 'airflow_db',
            'query': query,
            'args': {'date': '{{next_ds}}'},
        },
        dag=dag,
    )

    insert_into_eod_table = PythonOperator(
        task_id='insert_into_eod_table',
        provide_context=True,
        python_callable=insert_into_eod_table,
        dag=dag,
    )

    execute_eod_query >> insert_into_eod_table
