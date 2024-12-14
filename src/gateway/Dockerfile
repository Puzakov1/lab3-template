FROM python:3.11

COPY . /gateway
COPY requirements.txt /gateway
WORKDIR /gateway
RUN pip install -r requirements.txt

EXPOSE 8080
CMD gunicorn --bind 0.0.0.0:8080 app:app