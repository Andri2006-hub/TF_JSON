import json
import time
import psycopg2
from psycopg2.extras import Json
import pymongo

def create_nested_json(levels=15):
    data = "value"
    for i in range(levels):
        data = {f"level{levels - i}": data}
    return data

def insert_postgres(data):
    conn = psycopg2.connect(
        dbname="testdb",
        user="user",
        password="password",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS json_table (id SERIAL PRIMARY KEY, data JSONB)")
    start = time.time()
    cur.execute("INSERT INTO json_table (data) VALUES (%s)", (Json(data),))
    conn.commit()
    end = time.time()
    cur.close()
    conn.close()
    return end - start

def insert_mongo(data):
    client = pymongo.MongoClient("mongodb://root:password@localhost:27017/")
    db = client.testdb
    collection = db.json_collection
    start = time.time()
    collection.insert_one(data)
    end = time.time()
    client.close()
    return end - start

def query_postgres():
    conn = psycopg2.connect(
        dbname="testdb",
        user="user",
        password="password",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
    path = "->".join([f"'level{i}'" for i in range(1, 16)])
    query = f"SELECT data->{path} FROM json_table"
    start = time.time()
    cur.execute(query)
    result = cur.fetchone()
    end = time.time()
    cur.close()
    conn.close()
    return end - start, result[0] if result else None

def query_mongo():
    client = pymongo.MongoClient("mongodb://root:password@localhost:27017/")
    db = client.testdb
    collection = db.json_collection
    path = ".".join([f"level{i}" for i in range(1, 16)])
    projection = {path: 1, "_id": 0}
    start = time.time()
    result = collection.find_one({}, projection)
    end = time.time()
    client.close()
    return end - start, result

if __name__ == "__main__":
    data = create_nested_json(15)
    print("JSON criado com 15 níveis.")

    # Inserir no Postgres
    time_pg_insert = insert_postgres(data)
    print(f"Tempo de inserção no Postgres: {time_pg_insert:.4f}s")

    # Inserir no Mongo
    time_mongo_insert = insert_mongo(data)
    print(f"Tempo de inserção no Mongo: {time_mongo_insert:.4f}s")

    # Query no Postgres
    time_pg_query, result_pg = query_postgres()
    print(f"Tempo de query no Postgres: {time_pg_query:.4f}s, Resultado: {result_pg}")

    # Query no Mongo
    time_mongo_query, result_mongo = query_mongo()
    print(f"Tempo de query no Mongo: {time_mongo_query:.4f}s, Resultado: {result_mongo}")

    # Relatório comparativo
    print("\nRelatório Comparativo:")
    print(f"Inserção - Postgres: {time_pg_insert:.4f}s, Mongo: {time_mongo_insert:.4f}s")
    print(f"Query - Postgres: {time_pg_query:.4f}s, Mongo: {time_mongo_query:.4f}s")
    print("Dificuldade de query: Ambas requerem especificar o caminho completo, mas Postgres usa operadores ->, Mongo usa dot notation. Esforço similar para 15 níveis.")