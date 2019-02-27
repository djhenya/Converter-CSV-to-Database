import csv
import sys
from datetime import datetime

import psycopg2

from columns import COLUMN_NAMES


class Creator():
    '''
    Класс для копирования бухгалтерской (финансовой) отчетности предприятий и организаций (по данным Росстата) в формате .csv в БД PostgreSQL.
    '''

    def __init__(self, host, user_name, user_pwd):
        self.host = host
        self.user_name = user_name
        self.user_pwd = user_pwd

    def create_database(self, db_name):
        '''
        Функция для создания БД.
        '''
        try:
            connect_str = "dbname='postgres' user={} host={} password={}".format(self.user_name, self.host, self.user_pwd)
            con = psycopg2.connect(connect_str)
            con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            cur = con.cursor()
            cur.execute('''CREATE DATABASE {}'''.format(db_name))
            con.commit()
        except psycopg2.ProgrammingError as err:
            if con:
                con.rollback()
            print('Database creation error: ' + str(err))
        except Exception as e:
            if con:
                con.rollback()
            sys.exit(str(e))
        finally:
            if cur:
                cur.close()
            if con:
                con.close()
            del cur, con

    def create_table(self, db_name, port, table_name, columns_file, column_types, primary_keys):
        '''
        Функция для создания и разметки таблицы в БД для бухгалтерской (финансовой) отчетности предприятий и организаций (по данным Росстата)
        за ОДИН (!) год.
        При разметке используется файла формата данных в формате .csv .
        '''
        try:
            conn = psycopg2.connect(
                host=self.host,
                user=self.user_name,
                port=port,
                dbname=db_name
            )
        except Exception as e:
            # print(e)
            # return
            sys.exit(str(e))

        # Проверка наличия таблицы в базе
        try:
            cursor = conn.cursor()
            cursor.execute(
                '''SELECT EXISTS(SELECT 1 AS result FROM pg_tables WHERE tablename = %s);''', (table_name,))  # Проверка на существование таблицы в базе
        except Exception as e:
            if cursor:
                cursor.close()
            conn.rollback()
            conn.close()
            # print(e)
            # return
            sys.exit(str(e))

        if not cursor.fetchone()[0]: # Если таблица отсутствует в базе, то она создаётся.
            try:
                with open(columns_file, 'r') as csv_file:
                    csv_file.read(216)
                    reader = csv.DictReader(csv_file, delimiter=',')

                    statement = '''CREATE TABLE IF NOT EXISTS "{}" ('''.format(table_name)  # IF NOT EXISTS не нужен, но мало ли
                    for row in reader:

                        if row[' Имя поля '] == ' Тип отчета (0-СОНО':  # костыль
                            row[' Имя поля '] = ' Тип отчета                                        '
                            row[' Тип поля'] = ' C'
                            row[' Mаксимальная длина '] = '    1'

                        assert row[' Имя поля '] == COLUMN_NAMES[int(row[' N п/п'])-1]  # test_dev
                        column_name = row[' Имя поля ']
                        column_name = column_name.strip()
                        column_name = column_name.replace(' ', '_')
                        if int(row[' N п/п']) in range(9, 266):  # костыль
                            column_name = '_' + column_name

                        assert row[' Тип поля'] in column_types.keys()  # test_dev
                        column_type = column_types[row[' Тип поля']]

                        if column_type == 'varchar':
                            column_len = row[' Mаксимальная длина ']
                            statement += '{} {}({}),'.format(column_name,
                                                             column_type, column_len)
                        else:
                            statement += '{} {},'.format(column_name,
                                                         column_type)

                statement += '''PRIMARY KEY({})
                                );'''.format(primary_keys)

                # TODO: написать регулярку для ассерта  # test_dev
                # assert statement == '''CREATE TABLE IF NOT EXISTS "fs2016_dev" (
                #                         %s %s(%i),
                #                         %s %s(%i),
                #                         PRIMARY KEY(Наименование, ИНН)
                #                         );'''

                cursor.execute(statement) # Здесь выполняется один большой запрос (statement) на создание таблицы с 266 полями
                conn.commit()
            except Exception as e:
                if cursor:
                    cursor.close()
                conn.rollback()
                conn.close()
                # print(e)
                # return
                sys.exit(str(e))

        else: # Если таблица присутствует в базе, то она не пересоздается, а выводится следующее сообщение:
            print('Table creation error: table "{}" already exists'.format(table_name))

        if cursor:
            cursor.close()
        if conn:
            conn.close()

    def copy_from_csv(self, db_name, port, table_name, data_file):
        '''
        Функция для записи данных из .csv в таблицу, созданную и размеченную при помощи метода create_table.
        '''
        try:
            conn = psycopg2.connect(
                host=self.host,
                user=self.user_name,
                port=port,
                dbname=db_name
            )
            cursor = conn.cursor()
        except Exception as e:
            if cursor:
                cursor.close()
            if conn:
                conn.rollback()
                conn.close()
            # print(e)
            # return
            sys.exit(str(e))

        print('Copying started at {}'.format(datetime.now()))

        '''Вариант 1. Время выполнения для 2,5 млн строк: ~26 минут'''
        try:
            stm = '''COPY {} FROM %s DELIMITER ';' CSV QUOTE '^' ENCODING 'windows-1251';'''.format(table_name)
            cursor.execute(stm, (data_file,))
            print('Copying ended at {}'.format(datetime.now()))
            conn.commit()
        except Exception as e:
            conn.rollback()
            # print(e)
            sys.exit(str(e))
        finally:
            if cursor:
                cursor.close()
            conn.close()


        # '''Вариант 2. Время выполнения для 2,5 млн строк: ~39 минут'''
        # try:
        #     arr = []
        #     ss = r'%s, '*265 + r'%s'
        #     statement = '''INSERT INTO fs2016_dev VALUES(%s)''' %ss

        #     with open(data_file, 'r') as csv_file1:
        #         # dialect = csv.Sniffer().sniff(csv_file1.read(10240)) # автоопределение форматирования csv-файла
        #         # csv_file1.seek(0) # откат ридера к началу файла
        #         reader1 = csv.reader(csv_file1, delimiter='\n')

        #         for row1 in reader1:
        #             # r = row1[0].replace(';', '\', \'')
        #             r = row1[0].split(';')
        #             cursor.execute(statement, (r, )) # Вариант 2.1
        #             # arr.append(r) # Вариант 2.2

        #     # try:
        #     #     cursor.executemany(statement, arr # Вариант 2.2
        #     # except psycopg2.IntegrityError as e:
        #     #     conn.rollback()
        #     #     print(e)

        #     print(datetime.now())
        #     conn.commit()
        # except Exception as e:
        #     conn.rollback()
        #     print(e)
        # finally:
        #     if conn:
        #         conn.close()
