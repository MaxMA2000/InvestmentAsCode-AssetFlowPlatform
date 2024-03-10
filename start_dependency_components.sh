# start mongodb locally
# brew services start mongodb-community@7.0
sudo mongod --dbpath=/Users/maxma/data/db

# install & start airflow in local repo
# https://coder2j.com/airflow-tutorial/install-airflow-on-mac/
airflow standalone


# check port usage
sudo lsof -i :8080
# kill port application
sudo kill -9 99213
