if [ -z "${ENVIRONMENT}" ]
then
  behave ../features
else
  EXTERNAL_ENVIRONMENT="${ENVIRONMENT}"
  export ENVIRONMENT='DEV'
  python manage.py showmigrations
  behave ../features --logging-level=DEBUG
  export ENVIRONMENT="${EXTERNAL_ENVIRONMENT}"
fi