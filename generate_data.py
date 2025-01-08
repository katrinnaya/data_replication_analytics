import random
import string
from datetime import datetime, timedelta
import psycopg2

# Функция для генерации случайных строк
def generate_random_string(length):
    return ''.join(random.choice(string.ascii_letters) for _ in range(length))

# Функция для генерации уникальных имен и фамилий
def generate_unique_names():
    names = ["John", "Jane", "Michael", "Emily", "David"]
    surnames = ["Smith", "Johnson", "Williams", "Brown", "Jones"]
    return random.choice(names), random.choice(surnames)

# Функция для генерации дат в диапазоне
def generate_random_date(start, end):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return start + timedelta(seconds=random_second)

# Генерация данных для таблицы users
def generate_users(conn):
    cursor = conn.cursor()
    
    # Устанавливаем диапазон дат для регистрации пользователей
    start_reg_date = datetime.strptime('2020-01-01', '%Y-%m-%d')
    end_reg_date = datetime.now()
    
    for i in range(100):
        user_id = i + 1
        
        # Генерируем уникальные имена и фамилии
        first_name, last_name = generate_unique_names()
        
        email = f"{first_name.lower()}.{last_name.lower()}@example.com"
        phone = '+7' + ''.join(str(random.randint(0, 9)) for _ in range(10))
        
        # Генерируем случайную дату регистрации
        registration_date = generate_random_date(start_reg_date, end_reg_date).strftime('%Y-%m-%d %H:%M:%S')
        
        loyalty_status = random.choice(['Gold', 'Silver'])

        query = """
            INSERT INTO users (user_id, first_name, last_name, email, phone, registration_date, loyalty_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        values = (user_id, first_name, last_name, email, phone, registration_date, loyalty_status)
        cursor.execute(query, values)

    conn.commit()
    cursor.close()

# Генерация данных для таблицы products
def generate_products(conn):
    cursor = conn.cursor()
    
    # Устанавливаем диапазон дат для создания товаров
    start_creation_date = datetime.strptime('2018-01-01', '%Y-%m-%d')
    end_creation_date = datetime.now()
    
    categories = ['Electronics', 'Clothing', 'Books']
    for i in range(50):
        product_id = i + 1
        name = generate_random_string(20)
        description = generate_random_string(50)
        category_id = random.choice(categories)
        price = round(random.uniform(10, 500), 2)
        stock_quantity = random.randint(0, 100)
        
        # Генерируем случайную дату создания товара
        creation_date = generate_random_date(start_creation_date, end_creation_date).strftime('%Y-%m-%d %H:%M:%S')

        query = """
            INSERT INTO products (product_id, name, description, category_id, price, stock_quantity, creation_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        values = (product_id, name, description, category_id, price, stock_quantity, creation_date)
        cursor.execute(query, values)

    conn.commit()
    cursor.close()

# Генерация данных для таблицы orders
def generate_orders(conn):
    cursor = conn.cursor()
    
    # Устанавливаем диапазон дат для создания заказов
    start_order_date = datetime.strptime('2020-01-01', '%Y-%m-%d')
    end_order_date = datetime.now()
    
    statuses = ['Pending', 'Completed']
    for i in range(200):
        order_id = i + 1
        user_id = random.randint(1, 100)
        
        # Генерируем случайную дату заказа
        order_date = generate_random_date(start_order_date, end_order_date).strftime('%Y-%m-%d %H:%M:%S')
        
        total_amount = round(random.uniform(100, 1000), 2)
        status = random.choice(statuses)
        
        # Если статус заказа "Completed", устанавливаем дату доставки
        delivery_date = None if status == 'Pending' else (datetime.now() + timedelta(days=random.randint(1, 7))).strftime('%Y-%m-%d')

        query = """
            INSERT INTO orders (order_id, user_id, order_date, total_amount, status, delivery_date)
            VALUES (%s, %s, %s, %s, %s, %s);
        """
        values = (order_id, user_id, order_date, total_amount, status, delivery_date)
        cursor.execute(query, values)

    conn.commit()
    cursor.close()

# Генерация данных для таблицы order_details
def generate_order_details(conn):
    cursor = conn.cursor()
    
    for i in range(300):
        order_detail_id = i + 1
        order_id = random.randint(1, 200)
        product_id = random.randint(1, 50)
        quantity = random.randint(1, 5)
        price_per_unit = round(random.uniform(10, 150), 2)
        total_price = quantity * price_per_unit

        query = """
            INSERT INTO order_details (order_detail_id, order_id, product_id, quantity, price_per_unit, total_price)
            VALUES (%s, %s, %s, %s, %s, %s);
        """
        values = (order_detail_id, order_id, product_id, quantity, price_per_unit, total_price)
        cursor.execute(query, values)

    conn.commit()
    cursor.close()

# Генерация данных для таблицы product_categories
def generate_product_categories(conn):
    cursor = conn.cursor()
    
    categories = [
        {'id': 1, 'name': 'Electronics', 'parent_id': None},
        {'id': 2, 'name': 'Computers', 'parent_id': 1},
        {'id': 3, 'name': 'Phones', 'parent_id': 1},
        {'id': 4, 'name': 'Accessories', 'parent_id': 1},
        {'id': 5, 'name': 'Clothing', 'parent_id': None},
        {'id': 6, 'name': 'Men\'s Clothing', 'parent_id': 5},
        {'id': 7, 'name': 'Women\'s Clothing', 'parent_id': 5},
        {'id': 8, 'name': 'Kids\' Clothing', 'parent_id': 5},
        {'id': 9, 'name': 'Books', 'parent_id': None},
        {'id': 10, 'name': 'Fiction', 'parent_id': 9},
        {'id': 11, 'name': 'Non-Fiction', 'parent_id': 9}
    ]
    
    for cat in categories:
        query = """
            INSERT INTO product_categories (category_id, name, parent_category_id)
            VALUES (%s, %s, %s);
        """
        values = (cat['id'], cat['name'], cat['parent_id'])
        cursor.execute(query, values)

    conn.commit()
    cursor.close()

if __name__ == "__main__":
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="password",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="source_db")
        print("Connected to PostgreSQL successfully!")

        # Generate and insert data into tables
        generate_users(connection)
        generate_products(connection)
        generate_orders(connection)
        generate_order_details(connection)
        generate_product_categories(connection)

        connection.close()
        print("Data generation completed.")
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL:", error)
