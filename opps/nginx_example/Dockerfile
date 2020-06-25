FROM baseimage:latest

COPY . /project
WORKDIR /project

COPY default.conf /etc/nginx/sites-available/default
RUN mkdir -p /var/www/example/html
COPY index.html /var/www/example/html
COPY signals.html /var/www/example/html
#RUN ln -s /etc/nginx/sites-available/default /etc/nginx/sites-enabled/

EXPOSE 8080

STOPSIGNAL SIGTERM

#CMD ["nginx", "-g", "daemon off;"]
CMD ./start_nginx.sh -D & ./start_gunicorn.sh -D