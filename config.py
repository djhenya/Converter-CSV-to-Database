'''
Настройки для подключения к БД:
    'psql_host': 'str: имя хоста с базой данных',
    'username': 'str: имя пользователя (postgres)',
    'password': 'str: пароль пользователя',
    'connection_port': 'str: порт для коннекций при выполнении скрипта',
    'database_name': 'str: название базы данных',
    'years': список годов, записываемых в базу, например: [2012, 2013, 2017],
    'table_name': 'str: название создаваемых таблиц',
    'primary_keys_for_tables': 'str: перечисление желаемых первичных ключей в одну строку через запятую. Пример: "Наименование, ИНН"',
    'format_file_path': 'str: путь до директории с файлами формата (под названием "G2016.csv" - для 2016 года) в виде строки. Файлы для всех годов должны находиться в одной директории по данному пути.\
        Пример для Windows: "C:\\CSVtoDatabase\\formatFiles\\"',
    'data_file_path': 'str: путь до директории с файлами данных (под названием "data-20181029t000000-structure-20161231t000000.csv" - для 2016 года) в виде строки. Файлы для всех годов должны находиться в одной директории по данному пути.\
        Пример для Windows: "C:\\CSVtoDatabase\\dataFiles\\"',
'''

settings = {
    'psql_host': '',
    'username': '',
    'password': '',
    'connection_port': '',
    'database_name': '',
    'years': [],
    'table_name': '',
    'primary_keys_for_tables': '',
    'format_file_path': '',
    'data_file_path': '',
}