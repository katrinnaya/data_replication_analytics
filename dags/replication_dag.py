from airflow import DAG
from airflow.providers.postgres.operators.postgres_operator import PostgresOperator
from airflow.providers.mysql.operators.mysql_operator import MySqlOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['admin@example.com'],
    'email_on_failure':  True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='replication_dag',
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # Извлекаем данные из таблицы users в PostgreSQL
    replicate_users = PostgresOperator(
        task_id='replicate_users',
        sql="""SELECT * FROM users;""",
        postgres_conn_id='source_db',
        autocommit=True,
    )

    # Загружаем данные в MySQL
    load_users_to_mysql = MySqlOperator(
        task_id='load_users_to_mysql',
        mysql_conn_id='target_db',
        sql="""
            DROP TABLE IF EXISTS users;
            CREATE TABLE users (
                user_id INT AUTO_INCREMENT PRIMARY KEY,
                first_name VARCHAR(255),
                last_name VARCHAR(255),
                email VARCHAR(255),
                phone VARCHAR(20),
                registration_date DATETIME,
                loyalty_status VARCHAR(50)
            );
        """,
        autocommit=True,
    )

    # Извлекаем данные из таблицы products в PostgreSQL
    replicate_products = PostgresOperator(
        task_id='replicate_products',
        sql="""SELECT * FROM products;""",
        postgres_conn_id='source_db',
        autocommit=True,
    )

    # Загружаем данные в MySQL
    load_products_to_mysql = MySqlOperator(
        task_id='load_products_to_mysql',
        mysql_conn_id='target_db',
        sql="""
            DROP TABLE IF EXISTS products;
            CREATE TABLE products (
                product_id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                description TEXT,
                category_id INT,
                price DECIMAL(10, 2),
                stock_quantity INT,
                creation_date DATETIME
            );
        """,
        autocommit=True,
    )

    # Извлекаем данные из таблицы orders в PostgreSQL
    replicate_orders = PostgresOperator(
        task_id='replicate_orders',
        sql="""SELECT * FROM orders;""",
        postgres_conn_id='source_db',
        autocommit=True,
    )

    # Загружаем данные в MySQL
    load_orders_to_mysql = MySqlOperator(
        task_id='load_orders_to_mysql',
        mysql_conn_id='target_db',
        sql="""
            DROP TABLE IF EXISTS orders;
            CREATE TABLE orders (
                order_id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT,
                order_date DATETIME,
                total_amount DECIMAL(10, 2),
                status VARCHAR(50),
                delivery_date DATE
            );
        """,
        autocommit=True,
    )

    # Извлекаем данные из таблицы order_details в PostgreSQL
    replicate_order_details = PostgresOperator(
        task_id='replicate_order_details',
        sql="""SELECT * FROM order_details;""",
        postgres_conn_id='source_db',
        autocommit=True,
    )

    # Загружаем данные в MySQL
    load_order_details_to_mysql = MySqlOperator(
        task_id='load_order_details_to_mysql',
        mysql_conn_id='target_db',
        sql="""
            DROP TABLE IF EXISTS order_details;
            CREATE TABLE order_details (
                order_detail_id INT AUTO_INCREMENT PRIMARY KEY,
                order_id INT,
                product_id INT,
                quantity INT,
                price_per_unit DECIMAL(10, 2),
                total_price DECIMAL(10, 2)
            );
        """,
        autocommit=True,
    )

    # Извлекаем данные из таблицы product_categories в PostgreSQL
    replicate_product_categories = PostgresOperator(
        task_id='replicate_product_categories',
        sql="""SELECT * FROM product_categories;""",
        postgres_conn_id='source_db',
        autocommit=True,
    )

    # Загружаем данные в MySQL
    load_product_categories_to_mysql = MySqlOperator(
        task_id='load_product_categories_to_mysql',
        mysql_conn_id='target_db',
        sql="""
            DROP TABLE IF EXISTS product_categories;
            CREATE TABLE product_categories (
                category_id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                parent_category_id INT
            );
        """,
        autocommit=True,
    )

    # Выполняем последовательность задач
    replicate_users >> load_users_to_mysql
    replicate_products >> load_products_to_mysql
    replicate_orders >> load_orders_to_mysql
    replicate_order_details >> load_order_details_to_mysql
    replicate_product_categories >> load_product_categories_to_mysql
