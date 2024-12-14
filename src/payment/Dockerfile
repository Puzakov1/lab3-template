From python:3.11

COPY . /payment
COPY requirements.txt /payment
WORKDIR /payment
RUN pip install -r requirements.txt

EXPOSE 8060
CMD gunicorn --workers=4 --bind 0.0.0.0:8060 app:app