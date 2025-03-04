import psycopg2


class DBManager:
    def __init__(self, params: dict):
        self.params = params
        self.selected_companies = self.selecting_companies()

    @staticmethod
    def executing(cur):
        """Метод для вывода информации запрошенной из таблицы"""
        rows = cur.fetchall()
        for row in rows:
            print(row)
        return rows

    def selecting_companies(self):
        """10 интересных компаний"""
        conn = psycopg2.connect(**self.params)
        companies = []
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT company, COUNT(*) FROM vacancies "
                                "GROUP BY company "
                                "HAVING COUNT(*) > 1 "
                                "ORDER BY COUNT(*) DESC "
                                "LIMIT 10 ")
                    rows = cur.fetchall()
        finally:
            conn.close()
        for row in rows:
            companies.append(row[0])
        return companies

    def get_companies_and_vacancies_count(self):
        """получает список всех компаний и количество вакансий у каждой компании"""
        conn = psycopg2.connect(**self.params)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT company, COUNT(*) FROM vacancies "
                                "GROUP BY company "
                                "HAVING COUNT(*) > 1 "
                                "ORDER BY COUNT(*) DESC "
                                "LIMIT 10 ")
                    companies = self.executing(cur)
        finally:
            conn.close()
        return companies

    def get_all_vacancies(self):
        """получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию"""
        conn = psycopg2.connect(**self.params)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT company, name, salary_from, salary_to, url FROM vacancies")
                    companies = self.executing(cur)
        finally:
            conn.close()
        return companies

    def get_avg_salary(self):
        """получает среднюю зарплату по вакансиям"""
        conn = psycopg2.connect(**self.params)
        tpl_companies = tuple(self.selected_companies)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT company, AVG((salary_from + salary_to)/2) as total FROM vacancies "
                                f"WHERE company IN {tpl_companies} "
                                f"GROUP BY company "
                                f"ORDER BY total DESC")
                    self.executing(cur)
        finally:
            conn.close()

    def get_vacancies_with_higher_salary(self):
        """получает список всех вакансий, у которых зарплата выше средней по всем вакансиям"""
        conn = psycopg2.connect(**self.params)
        tpl_company = tuple(self.selected_companies)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT AVG((salary_from + salary_to)/2) FROM vacancies "
                                f"WHERE currency = 'RUR'")
                    avg_salary = cur.fetchall()
                    cur.execute(f"SELECT name FROM vacancies "
                                f"WHERE (salary_from + salary_to)/2 > {avg_salary[0][0]} "
                                f"AND company IN {tpl_company}")
                    companies = self.executing(cur)
        finally:
            conn.close()
        return companies

    def get_vacancies_with_keyword(self):
        """получает список всех вакансий, в названии которых содержатся переданные в метод слова,
         например python"""
        conn = psycopg2.connect(**self.params)
        print(f"Введите слово для отбора вакансий: ")
        word = input().lower().strip()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT name FROM vacancies "
                                f"WHERE name LIKE '%{word}%'")
                    vacancies = self.executing(cur)
                    if len(vacancies) == 0:
                        print(f"По вашему запросу ничего не найдено!")
        finally:
            conn.close()

        return vacancies