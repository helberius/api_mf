FROM tiangolo/uwsgi-nginx-flask:python3.7
RUN pip install elasticsearch flask-restful flask_cors

COPY ./app /app
