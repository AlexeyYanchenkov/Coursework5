from src.utils import get_vacancies_list, upload_to_database, information_output
from src.config import config
import os


def main():
    path_to_file = os.path.join('data', 'database.ini')
    par = config(path_to_file)
    vacancies_list = get_vacancies_list()
    upload_to_database(vacancies_list, par)
    information_output(par)


if __name__ == '__main__':
    main()