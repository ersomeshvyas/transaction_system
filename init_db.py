import sqlite3

connection = sqlite3.connect('database.db')


with open('schema.sql') as f:
    connection.executescript(f.read())

cur = connection.cursor()

cur.execute("INSERT INTO transactions (amount, account_no) VALUES (?, ?)", (200, 1234))

cur.execute("INSERT INTO transactions (amount, account_no) VALUES (?, ?)", (100, 2345))

connection.commit()
connection.close()
