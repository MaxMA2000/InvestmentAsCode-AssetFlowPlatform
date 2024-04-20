from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

from InvestmentAsCode_AssetFlowPlatform.jobs.crypto_pipeline import store_crypto_list
from InvestmentAsCode_AssetFlowPlatform.jobs.crypto_pipeline import store_daily_crypto_prices
from InvestmentAsCode_AssetFlowPlatform.jobs.crypto_pipeline import standardize_crypto_data

crypto_symbols = ["BTCUSD", "ETHUSD", "USDTUSD"]

schedule_interval = "0 17 * * *"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 4, 7),
}

with DAG(
    "crypto_price_pipeline",
    default_args=default_args,
    schedule_interval=schedule_interval,
) as dag:

    task_1 = PythonOperator(
        task_id="ingest_crypto_list",
        python_callable=store_crypto_list.task,
    )

    with TaskGroup("ingest_daily_crypto_prices") as ingest_daily_crypto_prices:
        tasks = []
        for crypto_symbol in crypto_symbols:
            ingest_task = PythonOperator(
                task_id=f"ingest_daily_crypto_price_{crypto_symbol}",
                python_callable=store_daily_crypto_prices.airflow_task,
                op_kwargs={"crypto_symbol": crypto_symbol}
            )

            standardize_task = PythonOperator(
                task_id=f"standardize_crypto_data_{crypto_symbol}",
                python_callable=standardize_crypto_data.airflow_task,
                op_kwargs={"crypto_symbol": crypto_symbol}
            )

            ingest_task >> standardize_task

            tasks.append(ingest_task)
            tasks.append(standardize_task)


        tasks  # Add all tasks to the task group

    task_1 >> ingest_daily_crypto_prices


