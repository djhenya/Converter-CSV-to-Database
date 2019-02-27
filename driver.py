import csv
import os

from config import settings
# from config_ import settings
from create_database_from_csv import Creator


'''
def data_check(csv_file_name): #пока не работает. TODO: проверка csv на ";;" и "/r;" или "/n;"

    new_csv_file_name = csv_file_name + '_tmp'

    with open(csv_file_name, 'r') as csv_file_read, open(new_csv_file_name, 'w') as csv_file_write:
        reader = csv.reader(csv_file_read, delimiter='\n')
        writer = csv.writer(csv_file_write) #delimiter='\n'
        for row in reader:
            r = row[0].split(';')
            print(r[0])
            if r[0] == '':
                continue
            else:
                writer.writerow(row)
            # print(row)
    
    os.rename(csv_file_name, csv_file_name + '_')
    os.rename(new_csv_file_name, csv_file_name)
'''


def csv_to_database():
    '''
    Main-функция, копирующая файлы бухгалтерской (финансовой) отчетности предприятий и организаций (по данным Росстата)
    за указанные в конфигурационном файле годы в базу данных под управление PostgreSQL.
    Файлы данных должны быть скачаны на жёсткий диск локального хоста в формате csv.
    Также файлы с описанием формата файлов данных должны быть скачаны на жёсткий диск локального хоста в формате csv.
    Все настройки указываются в файле config.py.
    '''

    data_types = {
        ' C': 'varchar',
        ' N': 'double precision'
    }

    psql_host = settings['psql_host']
    username = settings['username']
    password = settings['password']
    connection_port = settings['connection_port']
    database_name = settings['database_name']

    keys = settings['primary_keys_for_tables']
    table_name = settings['table_name']

    c = Creator(psql_host, username, password) # Инициализация объекта, осуществляющего копирование
    c.create_database(database_name) # Создание БД

    for year in settings['years']:
        table_name = table_name + str(year)

        format_description_file = settings['format_file_path'] + \
            "G{}.csv".format(year) # Файл с описанием формата файла данных
        if not os.path.isfile(format_description_file):
            raise OSError('Файл "{}" не найден.'.format(format_description_file))

        data_file = settings['data_file_path'] + \
            "data-20181029t000000-structure-{}1231t000000.csv".format(year) # Файл данных
        if not os.path.isfile(data_file):
            raise OSError('Файл "data-20181029t000000-structure-{}1231t000000.csv" не найден.'.format(year))

        c.create_table(database_name, connection_port, table_name,
                       format_description_file, data_types, keys) # Создание и разметка таблицы для отчётности за определённый год
        # data_check(data_file)
        c.copy_from_csv(database_name, connection_port, table_name, data_file) # Заполнение созданной таблицы данными
        print('{} - OK!\n\n'.format(year))


if __name__ == '__main__':
    csv_to_database()