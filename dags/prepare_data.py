from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values


def get_last_id(**kwargs):
    ti = kwargs['ti']

    get_query = 'SELECT id FROM prepared_data ORDER BY -id LIMIT 1;'
    pg_hook = PostgresHook(postgres_conn_id='gp_conn')
    con = pg_hook.get_conn()
    cur = con.cursor()
    cur.execute(get_query)
    data = cur.fetchone()
    if not data:
        data = (0,)

    ti.xcom_push(key='last_id', value=data[0])
    

def transfer_data(**kwargs):
    ti = kwargs['ti']

    pg_hook = PostgresHook(postgres_conn_id='gp_conn')
    con = pg_hook.get_conn()
    con.autocommit = True
    cur = con.cursor()

    last_id = ti.xcom_pull(key='last_id', task_ids=['get_last_id'])[0]

    cur.execute(f'SELECT * FROM raw_data WHERE id > {last_id} AND temperature >= 0;')
    data = cur.fetchall()

    query = f'INSERT INTO prepared_data (id, sensor_id, longitude, latitude, controller_id, datetime, temperature) VALUES %s;'

    execute_values(cur, query, data)


with DAG(
    dag_id='prepare_data',
    start_date=datetime(2022, 12, 15, 16, 45),
    schedule_interval='*/1 * * * *',
) as dag:
    
    start_step = EmptyOperator(task_id='start_step')
    
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='gp_conn',
        sql='sql/create_table.sql',
    )

    get_last_id = PythonOperator(
        task_id='get_last_id',
        python_callable=get_last_id,
    )

    transfer_data = PythonOperator(
        task_id='transfer_data',
        python_callable=transfer_data,
    )

    start_step >> create_table >> get_last_id >> transfer_data


if __name__ == "__main__":
    dag.cli()
