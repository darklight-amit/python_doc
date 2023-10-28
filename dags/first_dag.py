"""
Dedicated flow to generate M8 report process
"""

from datetime import datetime
from datetime import date
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.postgres.operators.postgres import PostgresOperator

"""
Below is Dag's property
:property::  'owner': 'airflow',
             'start_date': datetime(2023, 9, 6),
             'retries': 1,
"""
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 6),
    'retries': 1,
}

s3_report_path = str(date.today().year) + '/' + str(date.today().month) + '/' + str(date.today().day)

with DAG(
        dag_id='dev_m8_report_daily',
        default_args=default_args,
        schedule_interval=None,
) as dag:

    start_workflow = DummyOperator(task_id='start_workflow')

    load_m8_reporting_table = PostgresOperator(
        task_id="load_m8_reporting_table",
        postgres_conn_id="citris_postgres_conn_id",
        sql='sql/payment_monitoring/m8_transfer_return_payment_reconciliation.sql'
    )

    store_m8_report_into_s3 = SqlToS3Operator(
        task_id='store_m8_report_into_s3',
        sql_conn_id='citris_postgres_conn_id',
        query='select * from citrisfinancial_report.m8_transfer_return_payment_reconciliation where report_date = current_date',
        s3_bucket='dev-monitoring-control-reports',
        s3_key=s3_report_path + '/m8_transfer_return_payment_reconciliation.csv',
        replace=True,
    )
    

    

    end_workflow = DummyOperator(task_id='end_workflow')

    start_workflow >> load_m8_reporting_table >> store_m8_report_into_s3 >> end_workflow