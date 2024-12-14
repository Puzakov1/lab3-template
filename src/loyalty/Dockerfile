From python:3.11

COPY . /loyalty
COPY requirements.txt /loyalty
WORKDIR /loyalty
RUN pip install -r requirements.txt

EXPOSE 8050
CMD gunicorn --workers=4 --bind 0.0.0.0:8050 app:app