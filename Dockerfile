FROM python:3.9-slim-buster

# Установить необходимые пакеты
RUN apt-get update && apt-get install -y build-essential libpq-dev

# Копировать файлы проекта в контейнер
WORKDIR /app
COPY . .

# Установить зависимости Python
RUN pip install --no-cache-dir -r requirements.txt

# Установка дополнительных пакетов для Airflow
RUN pip install apache-airflow==2.2.5 \
                psycopg2-binary \
                mysql-connector-python

# Инициализация базы данных Airflow
RUN airflow db init

# Создать переменную окружения для хранения пароля
ARG AIRFLOW_ADMIN_PASSWORD
ENV AIRFLOW_ADMIN_PASSWORD=${AIRFLOW_ADMIN_PASSWORD}

# Создание пользователя admin для Airflow
RUN airflow users create \
    --username admin \
    --firstname Admin \
    --lastname Admin \
    --role Admin \
    --email admin@example.com \
    --password ${AIRFLOW_ADMIN_PASSWORD}

# Запуск веб-сервера Airflow
CMD ["airflow", "webserver", "-p", "8080"]
