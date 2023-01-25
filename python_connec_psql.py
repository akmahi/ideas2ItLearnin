import psycopg2
conn = psycopg2.connect(
    host="localhost",
    database="emp",
    user="postgres",
    password="postgrespw",
    port="32768")
cursor = conn.cursor()
cursor.execute("select * from accounts")
data = cursor.fetchall()
print(data)
conn.close()