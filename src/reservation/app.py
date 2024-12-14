import psycopg2

from flask import Flask, request, jsonify, make_response

from datetime import datetime

import uuid

app = Flask(__name__)

DB_URL = "postgresql://program:test@database:5432/reservations"
# DB_URL = "postgresql://postgres:postgres@database:5432/postgres"


@app.route('/manage/health', methods=['GET'])
def health_check():
    return {}, 200


@app.route('/api/v1/hotels', methods=['GET'])
def get_hotels():
    page = int(request.args["page"])
    size = int(request.args["size"])
    create_reservation_db()
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
select id, hotel_uid, name, country, city, address, stars, price from hotels
""")
            hotels = cursor.fetchall()
    hotel_list=[]
    for i, hotel in enumerate(hotels):
        if i < (page-1)*size:
            continue
        if i > page*size:
            break
        hotel_list.append(
            {
                "id":hotel[0],
                "hotelUid":hotel[1],
                "name":hotel[2],
                "country":hotel[3],
                "city":hotel[4],
                "address":hotel[5],
                "stars":hotel[6],
                "price":hotel[7]
            }
        )
    res = {"page": page, "pageSize": len(hotel_list), "totalElements": len(hotels), "items": hotel_list}
    return res, 200


@app.route('/api/v1/hotels/<hotel_id>', methods=['GET'])
def get_hotel_by_id(hotel_id:int):
    create_reservation_db()
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
select id, hotel_uid, name, country, city, address, stars, price from hotels where id = {hotel_id}
""")
            hotel = cursor.fetchone()
    return  {
                "id":hotel[0],
                "hotelUid":hotel[1],
                "name":hotel[2],
                "country":hotel[3],
                "city":hotel[4],
                "address":hotel[5],
                "stars":hotel[6],
                "price":hotel[7]
            }, 200


@app.route('/api/v1/hotels_by_uuid/<hotel_id>', methods=['GET'])
def get_hotel_by_uuid(hotel_id:str):
    create_reservation_db()
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
select id, hotel_uid, name, country, city, address, stars, price from hotels where hotel_uid = '{hotel_id}'
""")
            hotel = cursor.fetchone()
    return  {
                "id":hotel[0],
                "hotelUid":hotel[1],
                "name":hotel[2],
                "country":hotel[3],
                "city":hotel[4],
                "address":hotel[5],
                "stars":hotel[6],
                "price":hotel[7]
            }, 200


@app.route('/api/v1/reservations', methods=['POST'])
def post_reservation():
    create_reservation_db()
    user = request.headers['X-User-Name']
    body = request.json
    reservation_uid = uuid.uuid4()
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
select id from hotels where hotel_uid = '{body['hotelUid']}'
""")
            hotel_id = cursor.fetchone()[0]
            cursor.execute("select max(id) from reservation")
            max_id = cursor.fetchone()
            try:
                new_id = max_id[0]+1
            except:
                new_id = 1
            cursor.execute(f"""
insert into reservation (id, reservation_uid, username, payment_uid, hotel_id, status, start_date, end_data)
values ({new_id}, '{reservation_uid}', '{user}', '{body["paymentUid"]}', '{hotel_id}', 'PAID', '{body["startDate"]}', '{body["endDate"]}')
""")
            conn.commit()

    return {
        "id":new_id,
        "reservationUid":reservation_uid,
        "username":user,
        "paymentUid":body["paymentUid"],
        "hotel_id":hotel_id,
        "status":"PAID",
        "startDate":body["startDate"],
        "endDate":body["endDate"]
    }, 200



@app.route('/api/v1/reservations', methods=['GET'])
def get_reservations():
    user = request.headers['X-User-Name']
    create_reservation_db()
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
select id, reservation_uid, username, payment_uid, hotel_id, status, to_char(start_date, 'YYYY-MM-DD'), to_char(end_data, 'YYYY-MM-DD') from reservation
where username = '{user}'
""")
            reservations = cursor.fetchall()
    reservation_list=[]
    for reservation in reservations:
        reservation_list.append(
            {
                "id":reservation[0],
                "reservationUid":reservation[1],
                "username":reservation[2],
                "paymentUid":reservation[3],
                "hotel_id":reservation[4],
                "status":reservation[5],
                "startDate":reservation[6],
                "endDate":reservation[7]
            }
        )
    return reservation_list, 200


@app.route('/api/v1/reservations/<reservation_uuid>', methods=['GET'])
def get_reservation(reservation_uuid:str):
    user = request.headers['X-User-Name']
    create_reservation_db()
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
select id, reservation_uid, username, payment_uid, hotel_id, status, to_char(start_date, 'YYYY-MM-DD'), to_char(end_data, 'YYYY-MM-DD') from reservation
where username = '{user}' and reservation_uid = '{reservation_uuid}'
""")
            reservation = cursor.fetchone()
    if reservation is None:
        return {}, 404
    reservation =  {
        "id":reservation[0],
        "reservationUid":reservation[1],
        "username":reservation[2],
        "paymentUid":reservation[3],
        "hotel_id":reservation[4],
        "status":reservation[5],
        "startDate":reservation[6],
        "endDate":reservation[7]
    }
    return reservation, 200


@app.route('/api/v1/reservations/<reservation_uuid>', methods=['DELETE'])
def cancel_reservation(reservation_uuid:str):
    create_reservation_db()
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
update reservation set status = 'CANCELED' where reservation_uid = '{reservation_uuid}'
""")
            conn.commit()

            cursor.execute(f"""
SELECT payment_uid from reservation where reservation_uid = '{reservation_uuid}'
                           """)
            res = cursor.fetchone()
    return {"paymentUid":res[0]}, 200


def create_reservation_db():
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
CREATE TABLE if not exists hotels
(
    id        SERIAL PRIMARY KEY,
    hotel_uid uuid         NOT NULL UNIQUE,
    name      VARCHAR(255) NOT NULL,
    country   VARCHAR(80)  NOT NULL,
    city      VARCHAR(80)  NOT NULL,
    address   VARCHAR(255) NOT NULL,
    stars     INT,
    price     INT          NOT NULL
);

CREATE TABLE if not exists reservation
(
    id              SERIAL PRIMARY KEY,
    reservation_uid uuid UNIQUE NOT NULL,
    username        VARCHAR(80) NOT NULL,
    payment_uid     uuid        NOT NULL,
    hotel_id        INT REFERENCES hotels (id),
    status          VARCHAR(20) NOT NULL
        CHECK (status IN ('PAID', 'CANCELED')),
    start_date      TIMESTAMP WITH TIME ZONE,
    end_data        TIMESTAMP WITH TIME ZONE
);

""")
            conn.commit()
            cursor.execute(f"INSERT INTO hotels (id, hotel_uid, name, country, city, address, stars, price) "
                            f"VALUES (1, '049161bb-badd-4fa8-9d90-87c9a82b0668', 'Ararat Park Hyatt Moscow', 'Россия', 'Москва', 'Неглинная ул., 4', 5, 10000) on conflict do nothing;")
            conn.commit()
    return


if __name__ == '__main__':
    app.run(port=8070)
