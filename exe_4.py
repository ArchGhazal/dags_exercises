from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import timedelta

from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),}

create_query = """
CREATE TABLE employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    age INT NOT NULL);"""

insert_data_query = """
INSERT INTO employees (name, age) VALUES
    ('John Doe', 30),
    ('Jane Doe', 25),
    ('Bob Smith', 35);"""

calculating_average_age = """
SELECT AVG(age) FROM employees;"""

dag_postgres = DAG(
    dag_id="postgres_dag_connection",
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1))

create_table = PostgresOperator(
    task_id="creation_of_table",
    sql=create_query,
    dag=dag_postgres,
    postgres_conn_id="postgres_default")

insert_data = PostgresOperator(
    task_id="insertion_of_data",
    sql=insert_data_query,
    dag=dag_postgres,
    postgres_conn_id="postgres_default")

group_data = PostgresOperator(
    task_id="calculating_average_age",
    sql=calculating_average_age,
    dag=dag_postgres,
    postgres_conn_id="postgres_default")
create_table >> insert_data >> group_data
