import psycopg2

class DBManager:
    def __init__(self, params: dict):
        self.params = params
        self.create_database()
        self.create_tables()
        self.selected_companies = self.selecting_companies()

    def executing(self, cur):
        """Метод для вывода информации, запрошенной из таблицы"""
        rows = cur.fetchall()
        for row in rows:
            print(row)
        return rows

    def create_database(self):
        """Создает базу данных, если она не существует."""
        db_name = self.params["dbname"]
        temp_params = self.params.copy()
        temp_params.pop("dbname")  # Удаляем имя БД, чтобы подключиться к PostgreSQL без него

        conn = psycopg2.connect(**temp_params)
        conn.autocommit = True  # Разрешаем выполнять команды вне транзакции
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
                if not cur.fetchone():
                    cur.execute(f"CREATE DATABASE {db_name}")
                    print(f"База данных {db_name} создана.")
                else:
                    print(f"База данных {db_name} уже существует.")
        finally:
            conn.close()

    def create_tables(self):
        """Создает таблицы organizations и vacancies в БД."""
        conn = psycopg2.connect(**self.params)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS organizations (
                            id SERIAL PRIMARY KEY,
                            name VARCHAR(255) UNIQUE NOT NULL
                        )
                    """)
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS vacancies (
                            id SERIAL PRIMARY KEY,
                            company_id INTEGER REFERENCES organizations(id),
                            name VARCHAR(255) NOT NULL,
                            salary_from INTEGER,
                            salary_to INTEGER,
                            currency VARCHAR(10),
                            url TEXT
                        )
                    """)
                    print("Таблицы созданы или уже существуют.")
        finally:
            conn.close()

    def insert_organization(self, company_name):
        """Добавляет организацию в таблицу organizations, если её там нет."""
        conn = psycopg2.connect(**self.params)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO organizations (name) VALUES (%s) ON CONFLICT (name) DO NOTHING", (company_name,))
        finally:
            conn.close()

    def selecting_companies(self):
        """Выбирает 10 компаний с наибольшим числом вакансий."""
        conn = psycopg2.connect(**self.params)
        companies = []
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT company, COUNT(*)
                        FROM vacancies
                        GROUP BY company
                        HAVING COUNT(*) > 1
                        ORDER BY COUNT(*) DESC
                        LIMIT 10
                    """)
                    rows = cur.fetchall()
        finally:
            conn.close()

        companies = [row[0] for row in rows]
        return companies

    def get_companies_and_vacancies_count(self):
        """получает список всех компаний и количество вакансий у каждой компании"""
        conn = psycopg2.connect(**self.params)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT company, COUNT(*)
                        FROM vacancies
                        GROUP BY company
                        ORDER BY COUNT(*) DESC
                    """)
                    companies = self.executing(cur)
        finally:
            conn.close()
        return companies