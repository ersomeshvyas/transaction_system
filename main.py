# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import sqlite3
from flask import Flask, render_template, request, json
from flask_redis import FlaskRedis
import uuid

app = Flask(__name__)
redis_client = FlaskRedis(app)


def get_db_connection():
    conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row
    return conn


@app.route('/')
def index():
    conn = get_db_connection()
    posts = conn.execute('SELECT * FROM posts').fetchall()
    conn.close()
    return render_template('index.html', posts=posts)


@app.route('/transfer', methods=['POST'])
def transfer():
    data = request.get_json()
    from_account = data['from']
    to_account = data['to']
    amount = data['amount']

    # transaction_id = transaction['id']
    transaction_id = uuid.uuid4()

    # store transaction id in redis queue
    store_in_redis_queue(from_account, transaction_id)
    # send in queue consumer
    data = queue_consumer(transaction_id, data)
    # return message that transaction is queued
    response_obj = {
        "id": "transaction_id",
        "from": {
            "id": from_account,
            "balance": data['after_debit_amount']
        },
        "to": {
            "id": to_account,
            "balance": data['after_credit_amount']
        },
        "transfered": amount,
        "created_datetime": "transaction created time"
    }
    response = app.response_class(
        response=json.dumps(response_obj),
        mimetype='application/json'
    )
    return response


def store_in_redis_queue(from_account, transaction_id):
    redis_client.lpush(str(from_account)+"_transaction", str(transaction_id))


def queue_consumer(transaction_id, transaction):
    # store transaction obj in redis
    store_transaction_obj(transaction_id, transaction)
    # fetch record from queue
    transaction_id = fetch_record_from_queue(transaction['from'])
    # process record
    data = process_transaction(transaction_id)
    if not data:
        return None
    delete_record_from_queue(transaction['from'])
    return data


def store_transaction_obj(transaction_id, transaction):
    redis_client.set(str(transaction_id), json.dumps(transaction))


def fetch_record_from_queue(from_account):
    return redis_client.lindex(str(from_account)+"_transaction", 0)


def process_transaction(transaction_id):
    data = redis_client.get(transaction_id)
    transaction_obj = json.loads(data)
    conn = get_db_connection()

    from_account = conn.execute('SELECT * FROM transactions WHERE account_no = ?', (transaction_obj['from'],)).fetchone()
    to_account = conn.execute('SELECT * FROM transactions WHERE account_no = ?', (transaction_obj['to'],)).fetchone()

    print(from_account)
    print(to_account)

    after_credit_amount = to_account['amount'] + transaction_obj['amount']
    after_debit_amount = from_account['amount'] - transaction_obj['amount']

    if after_debit_amount < 0:
        conn.close()
        return None

    conn.execute('UPDATE transactions SET amount = ?'
                 ' WHERE id = ?',
                 (after_credit_amount, transaction_obj['to']))
    conn.execute('UPDATE transactions SET amount = ?'
                 ' WHERE id = ?',
                 (after_debit_amount, transaction_obj['from']))
    conn.commit()
    conn.close()
    redis_client.delete(str(transaction_id))
    obj = {'after_debit_amount': after_debit_amount, 'after_credit_amount': after_credit_amount}
    return obj


def delete_record_from_queue(from_account):
    return redis_client.lpop(str(from_account)+"_transaction")

