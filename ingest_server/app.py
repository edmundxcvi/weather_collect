from flask import Flask, request
from json import loads
# import psycopg2

app = Flask("ingest_server")

# def get_db_connection():
#     conn = psycopg2.connect("dbname=mydatabase user=myuser password=mypassword host=db")
#     return conn

@app.route('/data', methods=['POST'])
def save_data():
    data = loads(request.json)
    # conn = get_db_connection()
    # cur = conn.cursor()
    # # Insert data into a table
    # cur.execute("INSERT INTO data_table (field1, field2) VALUES (%s, %s)", (data['field1'], data['field2']))
    # conn.commit()
    # cur.close()
    # conn.close()
    print(data)
    return {'status': 'success'}, 201

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)