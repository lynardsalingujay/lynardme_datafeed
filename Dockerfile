# setup image and dir structure
FROM python:3.7.4-buster
COPY . /project

ARG ENVIRONMENT
ENV ENVIRONMENT=${ENVIRONMENT}
ENV DJANGO_SETTINGS_MODULE=settings

ENV DEV_DB=/project/opps/dev_db.sqlite3

RUN apt-get -y update
RUN apt-get install -y sqlite3
RUN sqlite3 ${DEV_DB} ""

WORKDIR /project/server
RUN pip install -r requirements.txt
RUN pip3 install gunicorn
RUN django-admin check
RUN python manage.py behave ../features --logging-level=DEBUG
