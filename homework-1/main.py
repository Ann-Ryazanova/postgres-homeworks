"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv

import psycopg2

employees_file = './north_data/employees_data.csv'
customers_file = './north_data/customers_data.csv'
orders_file = './north_data/orders_data.csv'

conn = psycopg2.connect(host="localhost", database="north", user="postgres", password="12345")

try:
    with conn:
        with conn.cursor() as cur:
            with open(employees_file, encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    cur.execute("INSERT INTO employees VALUES (default, %s, %s, %s, %s, %s)",
                                (row["first_name"], row["last_name"], row["title"], row["birth_date"], row["notes"]))

            with open(customers_file, encoding='windows-1251') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    cur.execute("INSERT INTO customers VALUES (%s, %s, %s)",
                                (row["customer_id"], row["company_name"], row["contact_name"]))

            with open(orders_file, encoding='windows-1251') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)",
                                (row["order_id"], row["customer_id"], row["employee_id"],
                                 row["order_date"], row["ship_city"]))

finally:
    conn.close()
