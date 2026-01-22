import os
from fastapi import  HTTPException, Body
from typing import List, Dict
import mysql.connector
from mysql.connector import Error

DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "mysql"),
    "user": os.getenv("MYSQL_USER", "weapon_warehouse_user"),
    "password": os.getenv("MYSQL_PASSWORD", "weapon_warehouse_pass"),
    "database": os.getenv("MYSQL_DATABASE", "weapon_warehouse"),
    "port": int(os.getenv("MYSQL_PORT", "3306"))
}

def get_db_connection():
    try:
        return mysql.connector.connect(**DB_CONFIG)
    except Error as e:
        raise HTTPException(status_code=503, detail=e)

def init_database():
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weapon_warehouse_records (
                id INT PRIMARY KEY AUTO_INCREMENT,
                weapon_id VARCHAR,
                weapon_name VARCHAR,
                weapon_type VARCHAR,
                range_km INT,
                weight_kg FLOAT,
                manufacturer VARCHAR,
                origin_country VARCHAR,
                storage_location VARCHAR,
                year_estimated INT,
                risk_level VARCHAR,
            )
        """)

        connection.commit()
        cursor.close()
        connection.close()
        return True

    except Error as e:
        print(e)
        return False

def store_records(data: List[Dict] = Body(...)):
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        insert_query = """
            INSERT INTO weapon_warehouse_records 
            (weapon_id, weapon_name, weapon_type, range_km, weight_kg, 
             manufacturer, origin_country, storage_location, year_estimated, risk_level)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        records_inserted = 0
        for record in data:
            values = (
                record.get("weapon_id"),
                record.get("weapon_name"),
                record.get("weapon_type"),
                record.get("range_km"),
                record.get("weight_kg"),
                record.get("manufacturer"),
                record.get("origin_country"),
                record.get("storage_location"),
                record.get("year_estimated"),
                record.get("risk_level")
            )

            cursor.execute(insert_query, values)
            records_inserted += 1

        connection.commit()
        cursor.close()
        connection.close()

        return {"status": "success", "inserted_records": records_inserted}

    except Error as e:
        raise HTTPException(status_code=500, detail=e)



