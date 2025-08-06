import psycopg2
from faker import Faker
from random import randint
from dotenv import load_dotenv
import os

# Завантажує змінні з файлу .env
load_dotenv()

# Налаштування підключення до бази даних
DB_NAME = "task_manager_db"
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = "localhost"
DB_PORT = "5432"

fake = Faker('uk_UA')

def create_connection():
    """Створює і повертає з'єднання з базою даних."""
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    return conn

def seed_data(conn):
    """Заповнює таблиці users та tasks випадковими даними."""
    cur = conn.cursor()

    # Очищення таблиць перед заповненням
    cur.execute("TRUNCATE TABLE tasks, users RESTART IDENTITY CASCADE;")

    # Заповнення таблиці users
    users_data = [(fake.name(), fake.email()) for _ in range(10)]
    cur.executemany("INSERT INTO users (fullname, email) VALUES (%s, %s)", users_data)

    # Отримання ID існуючих статусів
    cur.execute("SELECT id FROM status")
    status_ids = [row[0] for row in cur.fetchall()]

    # Отримання ID існуючих користувачів
    cur.execute("SELECT id FROM users")
    user_ids = [row[0] for row in cur.fetchall()]

    # Заповнення таблиці tasks
    tasks_data = []
    for _ in range(30):
        user_id = user_ids[randint(0, len(user_ids) - 1)]
        status_id = status_ids[randint(0, len(status_ids) - 1)]
        tasks_data.append((fake.sentence(nb_words=5), fake.text(), status_id, user_id))

    cur.executemany("INSERT INTO tasks (title, description, status_id, user_id) VALUES (%s, %s, %s, %s)", tasks_data)

    conn.commit()
    cur.close()

if __name__ == '__main__':
    try:
        conn = create_connection()
        print("Підключення до бази даних успішне!")
        seed_data(conn)
        print("База даних успішно заповнена випадковими даними.")
    except (Exception, psycopg2.Error) as error:
        print("Помилка під час роботи з PostgreSQL:", error)
    finally:
        if conn:
            conn.close()
            print("З'єднання з PostgreSQL закрите.")