
echo "starting Airflow"
export AIRFLOW_HOME=/Users/maxma/Desktop/InvestmentAsCode-Master-Repo/InvestmentAsCode-Asset-Flow-Platform/airflow
airflow standalone &

echo "starting MongoDB"
osascript -e 'tell application "Terminal" to do script "sudo mongod --dbpath=/Users/maxma/data/db"'


echo "starting PostgreSQL"
export PGDATA="/Users/maxma/Library/Application Support/Postgres/InvestmentAsCode/"
postgres

sleep 5s
echo "Local components start up complete"
