From python:3.11

COPY . /reservation
COPY requirements.txt /reservation
WORKDIR /reservation
RUN pip install -r requirements.txt

EXPOSE 8070
CMD gunicorn --workers=4 --bind 0.0.0.0:8070 app:app