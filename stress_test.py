import psycopg2
import time
import random
import os
from faker import Faker
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "dvdrental")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "postgres")

fake = Faker()

def fetch_ids(cur, table, column):
    try:
        cur.execute(f"SELECT {column} FROM {table}")
        return [row[0] for row in cur.fetchall()]
    except Exception as e:
        print(f"Failed to fetch IDs from {table}: {e}")
        return []

def run_stress_test(num_iterations):
    print(f"Connecting to Postgres at {DB_HOST}:{DB_PORT} db={DB_NAME}...")
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        conn.autocommit = True
        cur = conn.cursor()
        print("Connected successfully! Loading foreign key constraints in memory...")
        
        actor_ids = fetch_ids(cur, "actor", "actor_id")
        language_ids = fetch_ids(cur, "language", "language_id")
        country_ids = fetch_ids(cur, "country", "country_id")
        city_ids = fetch_ids(cur, "city", "city_id")
        address_ids = fetch_ids(cur, "address", "address_id")
        store_ids = fetch_ids(cur, "store", "store_id")
        customer_ids = fetch_ids(cur, "customer", "customer_id")
        staff_ids = fetch_ids(cur, "staff", "staff_id")
        film_ids = fetch_ids(cur, "film", "film_id")
        inventory_ids = fetch_ids(cur, "inventory", "inventory_id")
        rental_ids = fetch_ids(cur, "rental", "rental_id")

        print("Foreign keys loaded! Starting realistic stress test...")
        
        tables_to_test = [
            "actor", "country", "city", "address", 
            "customer", "film", "inventory", "rental", "payment"
        ]
        
        start_time = time.time()
        
        for i in range(1, num_iterations + 1):
            table = random.choices(
                tables_to_test, 
                weights=[5, 1, 2, 5, 5, 5, 10, 30, 37], 
                k=1
            )[0]
            
            try:
                if table == "actor":
                    first_name = fake.first_name().replace("'", "''")
                    last_name = fake.last_name().replace("'", "''")
                    cur.execute(f"INSERT INTO actor (first_name, last_name) VALUES ('{first_name}', '{last_name}') RETURNING actor_id;")
                    actor_ids.append(cur.fetchone()[0])
                    
                elif table == "country":
                    country = fake.country().replace("'", "''")
                    cur.execute(f"INSERT INTO country (country) VALUES ('{country}') RETURNING country_id;")
                    country_ids.append(cur.fetchone()[0])
                    
                elif table == "city":
                    if not country_ids: continue
                    city = fake.city().replace("'", "''")
                    country_id = random.choice(country_ids)
                    cur.execute(f"INSERT INTO city (city, country_id) VALUES ('{city}', {country_id}) RETURNING city_id;")
                    city_ids.append(cur.fetchone()[0])
                    
                elif table == "address":
                    if not city_ids: continue
                    address = fake.street_address().replace("'", "''")
                    district = fake.state().replace("'", "''")
                    city_id = random.choice(city_ids)
                    postal_code = fake.postcode()
                    phone = fake.phone_number()[:20]
                    cur.execute(f"INSERT INTO address (address, district, city_id, postal_code, phone) VALUES ('{address}', '{district}', {city_id}, '{postal_code}', '{phone}') RETURNING address_id;")
                    address_ids.append(cur.fetchone()[0])
                    
                elif table == "customer":
                    if not store_ids or not address_ids: continue
                    store_id = random.choice(store_ids)
                    first_name = fake.first_name().replace("'", "''")
                    last_name = fake.last_name().replace("'", "''")
                    email = fake.email().replace("'", "''")
                    address_id = random.choice(address_ids)
                    cur.execute(f"INSERT INTO customer (store_id, first_name, last_name, email, address_id, activebool) VALUES ({store_id}, '{first_name}', '{last_name}', '{email}', {address_id}, true) RETURNING customer_id;")
                    customer_ids.append(cur.fetchone()[0])
                    
                elif table == "film":
                    if not language_ids: continue
                    title = fake.catch_phrase().replace("'", "''")
                    desc = fake.text(max_nb_chars=100).replace("'", "''")
                    release_year = random.randint(1990, 2026)
                    lang_id = random.choice(language_ids)
                    rental_rate = round(random.uniform(0.99, 4.99), 2)
                    replacement_cost = round(random.uniform(9.99, 29.99), 2)
                    cur.execute(f"INSERT INTO film (title, description, release_year, language_id, rental_rate, replacement_cost) VALUES ('{title}', '{desc}', {release_year}, {lang_id}, {rental_rate}, {replacement_cost}) RETURNING film_id;")
                    film_ids.append(cur.fetchone()[0])
                    
                elif table == "inventory":
                    if not film_ids or not store_ids: continue
                    film_id = random.choice(film_ids)
                    store_id = random.choice(store_ids)
                    cur.execute(f"INSERT INTO inventory (film_id, store_id) VALUES ({film_id}, {store_id}) RETURNING inventory_id;")
                    inventory_ids.append(cur.fetchone()[0])
                    
                elif table == "rental":
                    if not inventory_ids or not customer_ids or not staff_ids: continue
                    inv_id = random.choice(inventory_ids)
                    cust_id = random.choice(customer_ids)
                    staff_id = random.choice(staff_ids)
                    rental_date = fake.date_time_this_year().strftime('%Y-%m-%d %H:%M:%S')
                    cur.execute(f"INSERT INTO rental (rental_date, inventory_id, customer_id, staff_id) VALUES ('{rental_date}', {inv_id}, {cust_id}, {staff_id}) RETURNING rental_id;")
                    rental_ids.append(cur.fetchone()[0])
                    
                elif table == "payment":
                    if not customer_ids or not staff_ids or not rental_ids: continue
                    cust_id = random.choice(customer_ids)
                    staff_id = random.choice(staff_ids)
                    rental_id = random.choice(rental_ids)
                    amount = round(random.uniform(1.99, 9.99), 2)
                    payment_date = fake.date_time_this_year().strftime('%Y-%m-%d %H:%M:%S')
                    cur.execute(f"INSERT INTO payment (customer_id, staff_id, rental_id, amount, payment_date) VALUES ({cust_id}, {staff_id}, {rental_id}, {amount}, '{payment_date}')")
                
                if i % 100 == 0:
                    print(f"[{time.strftime('%H:%M:%S')}] Processed {i}/{num_iterations} operations...")
                    
                time.sleep(random.uniform(0.01, 0.05))
                
            except Exception as e:
                print(f"Skipping failed insert on {table}: {e}")
                conn.rollback()
                
        end_time = time.time()
        print(f"✅ Stress test completed in {end_time - start_time:.2f} seconds!")
        
    except Exception as e:
        print(f"❌ Critical Error: {e}")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()
        print("Database connection closed.")

if __name__ == "__main__":
    TOTAL_ITERATIONS = 2000
    print("Starting DVD Rental Streaming Data Generator...")
    run_stress_test(TOTAL_ITERATIONS)
