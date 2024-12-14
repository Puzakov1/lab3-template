import psycopg2

from flask import Flask, request, jsonify, make_response
import json
import uuid

app = Flask(__name__)

DB_URL = "postgresql://program:test@database:5432/payments"
# DB_URL = "postgresql://postgres:postgres@database:5432/postgres"


@app.route('/manage/health', methods=['GET'])
def health_check():
    return {}, 200


@app.route('/api/v1/payment', methods=['POST'])
def create_payment():
    create_payment_db()
    payment_uuid = uuid.uuid4()
    body = request.json
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select max(id) from payment")
            max_id = cursor.fetchone()
            try:
                new_id = max_id[0]+1
            except:
                new_id = 1
            cursor.execute(f"""
insert into payment (id, payment_uid, status, price) values ({new_id}, '{payment_uuid}', 'PAID', {int(body["price"])})
""")
            conn.commit()
    return {
        "id":new_id,
        "paymentUid":payment_uuid,
        "status":"PAID",
        "price":body["price"]
    }, 200


@app.route('/api/v1/payment/cancel/<payment_uuid>', methods=['PATCH'])
def cancel_payment(payment_uuid:str):
    create_payment_db()
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
update payment set status = 'CANCELED' where payment_uid = '{payment_uuid}'
""")
            conn.commit()
    return "CANCELED", 200


@app.route('/api/v1/payment/<payment_uuid>', methods=['GET'])
def get_payment(payment_uuid:str):
    create_payment_db()
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
select id, payment_uid, status, price from payment where payment_uid = '{payment_uuid}'
""")
            payment = cursor.fetchone()
    if payment is None:
        return {}, 404
    payment = {
        "id":payment[0],
        "payment_uid":payment[1],
        "status":payment[2],
        "price":payment[3]
    }
    return payment, 200


def create_payment_db():
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
CREATE TABLE if not exists payment
(
    id          SERIAL PRIMARY KEY,
    payment_uid uuid        NOT NULL,
    status      VARCHAR(20) NOT NULL
        CHECK (status IN ('PAID', 'CANCELED')),
    price       INT         NOT NULL
);
""")
            conn.commit()
    return


if __name__ == '__main__':
    app.run(port=8060)
