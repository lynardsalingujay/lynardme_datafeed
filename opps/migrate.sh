if [ -z "${ENVIRONMENT}" ]
then
  export ENVIRONMENT='DEV'
fi

if [ -z "${DEV_DB}" ]
then
  export DEV_DB='~/dev_db.sqlite3'
fi

if [ "$ENVIRONMENT" = 'BUILD' ] || [ "$ENVIRONMENT" = 'PRODUCTION' ]
then
  if [ -f "cloud_sql_proxy" ]
  then
    echo "cloud_sql_proxy already downloaded"
  else
    wget https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64 -O ../opps/cloud_sql_proxy
    chmod +x ../opps/cloud_sql_proxy
  fi
  ../opps/cloud_sql_proxy -dir=/cloudsql/ -instances=datafeed-247709:asia-northeast1:shiny-production -credential_file=../opps/creds.json &
  sleep 10
  python manage.py migrate
elif [ "$ENVIRONMENT" = 'DEV' ]
then
  python manage.py migrate
  python manage.py createcachetable
fi