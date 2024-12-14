import psycopg2

from flask import Flask, request, jsonify, make_response

app = Flask(__name__)

DB_URL = "postgresql://program:test@database:5432/loyalties"
# DB_URL = "postgresql://postgres:postgres@database:5432/postgres"

@app.route('/manage/health', methods=['GET'])
def health_check():
    return {}, 200


@app.route('/api/v1/loyalty/add', methods=['PATCH'])
def increase_loyalty():
    create_loyalty_db()
    user = request.headers['X-User-Name']
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
select reservation_count from loyalty loy where loy.username = '{user}'
""")
            loyalty = cursor.fetchone()
            if loyalty is None:
                return {}, 404

            reservation_count = loyalty[0]+1
            if reservation_count < 10:
                status = "BRONZE"
                discount = 5
            elif reservation_count< 20:
                status = "SILVER"
                discount = 7
            else:
                status = "GOLD"
                discount = 10
            cursor.execute(f"""
update loyalty set discount = '{discount}', status='{status}', reservation_count={reservation_count} where username = '{user}'
""")
            conn.commit()
    loyalty = {
        "status":status,
        "discount":discount,
        "reservation_count":reservation_count
    }
    return loyalty, 200


@app.route('/api/v1/loyalty/remove', methods=['PATCH'])
def decrease_loyalty():
    create_loyalty_db()
    user = request.headers['X-User-Name']
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
select reservation_count from loyalty loy where loy.username = '{user}'
""")
            loyalty = cursor.fetchone()
            if loyalty is None:
                return {}, 404

            reservation_count = loyalty[0]-1
            if reservation_count < 10:
                status = "BRONZE"
                discount = 5
            elif reservation_count< 20:
                status = "SILVER"
                discount = 7
            else:
                status = "GOLD"
                discount = 10
            cursor.execute(f"""
update loyalty set discount = '{discount}', status='{status}', reservation_count={reservation_count} where username = '{user}'
""")
            conn.commit()
    loyalty = {
        "status":status,
        "discount":discount,
        "reservation_count":reservation_count
    }
    return loyalty, 200


@app.route('/api/v1/loyalty', methods=['POST'])
def add_loyalty(user:str):
    create_loyalty_db()
    user = request.headers['X-User-Name']
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select max(id) from loyalty")
            max_id = cursor.fetchone()
            if max_id is None:
                max_id = 0
            else:
                max_id = max_id[0]
            cursor.execute(f"""
insert into loyalty (id, username, reservation_count, status, discount) values ({max_id + 1}, '{user}', 0, 'BRONZE', 5)
""")
            conn.commit()
    return {
        "id":max_id + 1,
        "username":user,
        "status":"BRONZE",
        "discount":5,
        "reservation_count":0
    }, 200


@app.route('/api/v1/loyalty', methods=['GET'])
def get_loyalty():
    create_loyalty_db()
    user = request.headers['X-User-Name']
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
select status, discount, reservation_count from loyalty loy where loy.username = '{user}'
""")
            loyalty = cursor.fetchone()
    if loyalty is None:
        return {}, 404
    loyalty = {
        "status":loyalty[0],
        "discount":loyalty[1],
        "reservationCount":loyalty[2]
    }
    return loyalty, 200


def create_loyalty_db():
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
CREATE TABLE if not exists loyalty
(
    id                SERIAL PRIMARY KEY,
    username          VARCHAR(80) NOT NULL UNIQUE,
    reservation_count INT         NOT NULL DEFAULT 0,
    status            VARCHAR(80) NOT NULL DEFAULT 'BRONZE'
        CHECK (status IN ('BRONZE', 'SILVER', 'GOLD')),
    discount          INT         NOT NULL
);
""")
            conn.commit()
            cursor.execute(f"INSERT INTO loyalty (id, username, reservation_count, status, discount) "
                            f"VALUES (1, 'Test Max', 25, 'GOLD', 10) on conflict do nothing;")
            conn.commit()
    return


if __name__ == '__main__':
    app.run(port=8050)
