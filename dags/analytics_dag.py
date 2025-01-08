from airflow import DAG
from airflow.providers.mysql.operators.mysql_get_records import MySqlGetRecordsOperator
from airflow.providers.mysql.operators.mysql_operator import MySqlOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='analytics_dag',
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # Задача для создания витрины активности пользователей
    create_user_activity_viz = MySqlOperator(
        task_id='create_user_activity_viz',
        mysql_conn_id='target_db',
        sql="""
            DROP TABLE IF EXISTS user_activity_viz;
            CREATE TABLE user_activity_viz AS
            SELECT
                u.user_id,
                u.first_name,
                u.last_name,
                COUNT(o.order_id) AS number_of_orders,
                SUM(o.total_amount) AS total_spent,
                MAX(o.order_date) AS last_order_date
            FROM users u
            LEFT JOIN orders o ON u.user_id = o.user_id
            GROUP BY u.user_id, u.first_name, u.last_name;
        """,
        autocommit=True,
    )

    # Задача для создания витрины продаж
    create_sales_viz = MySqlOperator(
        task_id='create_sales_viz',
        mysql_conn_id='target_db',
        sql="""
            DROP TABLE IF EXISTS sales_viz;
            CREATE TABLE sales_viz AS
            SELECT
                p.product_id,
                p.name,
                SUM(od.quantity) AS total_sold,
                SUM(od.total_price) AS revenue
            FROM products p
            INNER JOIN order_details od ON p.product_id = od.product_id
            GROUP BY p.product_id, p.name;
        """,
        autocommit=True,
    )

    # Задачи выполняются последовательно
    create_user_activity_viz >> create_sales_viz
