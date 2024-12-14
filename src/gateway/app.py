from flask import Flask, request, jsonify, make_response
import requests
from datetime import datetime as dt

app = Flask(__name__)


@app.route('/manage/health', methods=['GET'])
def health_check():
    return {}, 200


@app.route('/api/v1/hotels', methods=['GET'])
def get_hotels():
    response = requests.get('http://reservation:8070/api/v1/hotels?' + request.full_path.split('?')[-1])
    return response.json(), 200


@app.route('/api/v1/me', methods=['GET'])
def get_me():
    user = request.headers['X-User-Name']

    response = requests.get("http://reservation:8070/api/v1/reservations", headers={'X-User-Name': user})

    reservations = response.json()
    for res in reservations:
        response = requests.get('http://reservation:8070/api/v1/hotels/' + str(res['hotel_id']))
        
        hotel = response.json()
        hotel["fullAddress"] = f"{hotel['country']}, {hotel['city']}, {hotel['address']}"
        del hotel['country']
        del hotel['city']
        del hotel['address']
        
        res['hotel'] = hotel

        response = requests.get('http://payment:8060/api/v1/payment/' + res['paymentUid'])

        del res['paymentUid']
        res['payment'] = response.json()

    response = requests.get('http://loyalty:8050/api/v1/loyalty', headers={'X-User-Name': user})

    loyalty = response.json()
    result = {
        'reservations': reservations,
        'loyalty': loyalty
    }
    return result, 200


@app.route('/api/v1/reservations', methods=['GET'])
def get_reservations():
    user = request.headers['X-User-Name']

    response = requests.get("http://reservation:8070/api/v1/reservations", headers={'X-User-Name': user})

    reservations = response.json()
    for res in reservations:
        response = requests.get('http://reservation:8070/api/v1/hotels/' + str(res['hotel_id']))
        hotel = response.json()

        hotel["fullAddress"] = f"{hotel['country']}, {hotel['city']}, {hotel['address']}"
        del hotel['country']
        del hotel['city']
        del hotel['address']

        res['hotel'] = hotel

        response = requests.get('http://payment:8060/api/v1/payment/' + res['paymentUid'])

        del res['paymentUid']
        res['payment'] = response.json()
    return reservations, 200


@app.route('/api/v1/reservations/<reservationUid>', methods=['GET'])
def get_reservation(reservationUid: str):
    user = request.headers['X-User-Name']
    response = requests.get("http://reservation:8070/api/v1/reservations/" + reservationUid, headers={'X-User-Name': user})

    reservation = response.json()

    response = requests.get('http://reservation:8070/api/v1/hotels/' + str(reservation['hotel_id']))

    del reservation['hotel_id']
    
    hotel = response.json()
    hotel["fullAddress"] = f"{hotel['country']}, {hotel['city']}, {hotel['address']}"
    del hotel['country']
    del hotel['city']
    del hotel['address']
    reservation['hotel'] = hotel
    response = requests.get('http://payment:8060/api/v1/payment/' + reservation['paymentUid'])

    del reservation['paymentUid']
    reservation['payment'] = response.json()

    return reservation, 200


@app.route('/api/v1/reservations', methods=['POST'])
def post_reservations():
    user = request.headers['X-User-Name']
    body = request.json

    response = requests.get('http://reservation:8070/api/v1/hotels_by_uuid/' + body['hotelUid'])

    hotel = response.json()
    price = (
        dt.strptime(body['endDate'], "%Y-%m-%d").date() 
        - dt.strptime(body['startDate'], "%Y-%m-%d").date()
    ).days * hotel['price']

    response = requests.get('http://loyalty:8050/api/v1/loyalty', headers={'X-User-Name': user})
    loyalty = response.json()
    discount = loyalty['discount']

    price_with_discount = int(price * (1 - discount / 100))

    response = requests.post(
        'http://payment:8060/api/v1/payment',
        headers={'X-User-Name': user, 'Content-Type': 'application/json'}, json={'price': price_with_discount},)

    payment = response.json()

    response = requests.patch(
        'http://loyalty:8050/api/v1/loyalty/add',
        headers={'X-User-Name': user})

    loyalty = response.json()

    response = requests.post(
        "http://reservation:8070/api/v1/reservations",
        headers={'X-User-Name': user,
                 'Content-Type': 'application/json'},
        json={
            'hotelUid': hotel['hotelUid'], 
            'startDate': body['startDate'],
            'endDate': body['endDate'],
            'paymentUid': payment['paymentUid']
        })

    reservation = response.json()

    del reservation['hotel_id']
    reservation['hotelUid'] = hotel['hotelUid']
    del reservation['username']
    reservation['discount'] = discount
    del reservation['paymentUid']
    del payment['paymentUid']
    del payment['id']
    reservation['payment'] = payment

    return reservation, 200


@app.route('/api/v1/reservations/<reservationUid>', methods=['DELETE'])
def delete_reservation(reservationUid: str):
    user = request.headers['X-User-Name']
    response = requests.delete("http://reservation:8070/api/v1/reservations/" + reservationUid)

    reservation = response.json()

    response = requests.patch('http://payment:8060/api/v1/payment/' + reservation['paymentUid'])

    response = requests.patch(
        'http://loyalty:8050/api/v1/loyalty/remove',
        headers={'X-User-Name': user}
    )

    return {}, 204


@app.route('/api/v1/loyalty', methods=['GET'])
def get_loyalty():
    user = request.headers['X-User-Name']

    response = requests.get('http://loyalty:8050/api/v1/loyalty', headers={'X-User-Name': user})

    return response.json(), 200

if __name__ == '__main__':
    app.run(port=8050)
