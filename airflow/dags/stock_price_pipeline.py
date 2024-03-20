from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

from InvestmentAsCode_AssetFlowPlatform.jobs.stock_pipeline import store_stock_list
from InvestmentAsCode_AssetFlowPlatform.jobs.stock_pipeline import store_daily_stock_prices
from InvestmentAsCode_AssetFlowPlatform.jobs.stock_pipeline import standardize_stock_data

stock_symbols = ["AAPL", "NVDA", "TSLA"]

schedule_interval = "0 16 * * *"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 8),
}

with DAG(
    "stock_price_pipeline",
    default_args=default_args,
    schedule_interval=schedule_interval,
) as dag:

    task_1 = PythonOperator(
        task_id="ingest_stock_list",
        python_callable=store_stock_list.task,
    )

    with TaskGroup("ingest_daily_stock_prices") as ingest_daily_stock_prices:
        tasks = []
        for stock_symbol in stock_symbols:
            ingest_task = PythonOperator(
                task_id=f"ingest_daily_stock_price_{stock_symbol}",
                python_callable=store_daily_stock_prices.airflow_task,
                op_kwargs={"stock_symbol": stock_symbol}
            )

            standardize_task = PythonOperator(
                task_id=f"standardize_stock_data_{stock_symbol}",
                python_callable=standardize_stock_data.airflow_task,
                op_kwargs={"stock_symbol": stock_symbol}
            )

            ingest_task >> standardize_task

            tasks.append(ingest_task)
            tasks.append(standardize_task)


        tasks  # Add all tasks to the task group

    task_1 >> ingest_daily_stock_prices


