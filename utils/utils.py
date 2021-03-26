import zlib
import sqlite3
import random
import datetime
import psycopg2


def compress_data(data):
    return zlib.compress(data, level=9)


def decompress_data(data):
    return zlib.decompress(data)


def pad_message(message, padding_size):
    while len(message) % padding_size:
        message += b' '

    return message


def de_pad_message(message):
    return message.rstrip(b' ')


class DataGenerator:
    fields = [('client_id', 'int'),
              ('client_fio', 'varchar(100)'),
              ('client_phone', 'varchar(20)'),
              ('specialist_id', 'int'),
              ('specialist_fio', 'varchar(100)'),
              ('specialist_area_id', 'int'),
              ('area_id', 'int'),
              ('area_name', 'varchar(50)'),
              ('medical_info_id', 'int'),
              ('medical_info_client_id', 'int'),
              ('medical_info_date', 'date'),
              ('medical_info_complaint', 'text'),
              ('medical_info_treatment', 'text'),
              ('record_id', 'int'),
              ('record_client_id', 'int'),
              ('record_specialist_id', 'int'),
              ('record_date', 'datetime'),
              ('record_price', 'numeric'),
              ('record_medical_info_id', 'int')
              ]

    @staticmethod
    def generate_db(db, table_name, table_fields=None):
        if not table_fields:
            table_fields = DataGenerator.fields
        with sqlite3.connect(db) as conn:
            cursor = conn.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
            cursor.execute(f"""CREATE TABLE {table_name}
                              ({", ".join([f'{x[0]} {x[1]}' for x in table_fields])});
                           """)
            conn.commit()

    @staticmethod
    def generate_data(amount, db, table_name):
        areas = {0: 'Хирургия', 1: 'Детская стоматология', 2: 'Ортодонтия', 3: 'Имплантология'}
        clients = {}
        for i in range(amount):
            clients[i] = DataGenerator._generate_random_client_()
        specialists = {}
        for i in range(amount):
            specialists[i] = DataGenerator._generate_random_specialist_(len(areas) - 1)

        with sqlite3.connect(db) as conn:
            cursor = conn.cursor()
            for i in range(amount):
                client_id = random.choice(list(clients.keys()))
                specialist_id = random.choice(list(specialists.keys()))
                data = {
                    'client_id': client_id,
                    'client_fio': clients[client_id][0],
                    'client_phone': clients[client_id][1],
                    'specialist_id': specialist_id,
                    'specialist_fio': specialists[specialist_id][0],
                    'specialist_area_id': specialists[specialist_id][1],
                    'area_id': specialists[specialist_id][1],
                    'area_name': areas[specialists[specialist_id][1]],
                    'medical_info_id': i,
                    'medical_info_client_id': client_id,
                    'medical_info_date': datetime.datetime.now(),
                    'medical_info_complaint': f'Some complaint {random.randint(0, i)}',
                    'medical_info_treatment': f'Some treatment {random.randint(0, i)}',
                    'record_id': i,
                    'record_client_id': client_id,
                    'record_specialist_id': specialist_id,
                    'record_date': datetime.datetime.now(),
                    'record_price': random.randint(1000, 10000),
                    'record_medical_info_id': i,
                }
                sql = f"""INSERT INTO {table_name} (client_id, client_fio, client_phone, specialist_id, specialist_fio, 
                        specialist_area_id, area_id, area_name, medical_info_id, medical_info_client_id, 
                        medical_info_date, medical_info_complaint, medical_info_treatment, record_id, 
                        record_client_id, record_specialist_id, record_date, record_price, record_medical_info_id)
                        VALUES (:client_id, :client_fio, :client_phone, :specialist_id, :specialist_fio, 
                        :specialist_area_id, :area_id, :area_name, :medical_info_id, :medical_info_client_id, 
                        :medical_info_date, :medical_info_complaint, :medical_info_treatment, :record_id, 
                        :record_client_id, :record_specialist_id, :record_date, :record_price, :record_medical_info_id);"""
                cursor.execute(sql, data)
            conn.commit()

    @staticmethod
    def get_all_data(db, table_name):
        with sqlite3.connect(db) as conn:
            cursor = conn.cursor()
            return cursor.execute(f"SELECT * FROM {table_name};")

    @staticmethod
    def _generate_random_client_():
        names = ['Иванов И.И.', 'Гилёв Е.Д.', 'Степанов В.В.', 'Еловикова А.И.', 'Рыжов Н.С.', 'Граничникова А.С.']
        phone = '+7' + ''.join(random.choices('1234567890', k=10))
        return tuple((random.choice(names), phone))

    @staticmethod
    def _generate_random_specialist_(areas_max_id):
        names = ['Иванов И.И.', 'Гилёв Е.Д.', 'Степанов В.В.', 'Еловикова А.И.', 'Рыжов Н.С.', 'Граничникова А.С.']
        area_id = random.randint(0, areas_max_id)
        return tuple((random.choice(names), area_id))


class DataParser:
    @staticmethod
    def check_db(db_connection):
        tables_mapping = {
            'clients': ['id INTEGER, PRIMARY KEY (id)', 'fio VARCHAR(100)', 'phone VARCHAR(20)'],
            'areas': ['id INTEGER, PRIMARY KEY (id)', 'name VARCHAR(100)'],
            'specialists': ['id INTEGER, PRIMARY KEY (id)', 'fio VARCHAR(100)',
                            'area_id INTEGER, FOREIGN KEY (area_id) REFERENCES areas (id)'],
            'medical_info': ['id INTEGER, PRIMARY KEY (id)', 'client_id INTEGER, FOREIGN KEY (client_id) REFERENCES clients (id)',
                             'date TIMESTAMP', 'compliant TEXT', 'treatment TEXT'],
            'records': ['id INTEGER, PRIMARY KEY (id)', 'client_id INTEGER, FOREIGN KEY (client_id) REFERENCES clients (id)',
                        'specialist_id INTEGER, FOREIGN KEY (specialist_id) REFERENCES specialists (id)',
                        'medical_info_id INTEGER, FOREIGN KEY (medical_info_id) REFERENCES medical_info (id)',
                        'price INTEGER', 'date TIMESTAMP']
        }
        cursor = db_connection.cursor()
        for table in list(tables_mapping.keys()):
            cursor.execute(f"""SELECT EXISTS(
                                                SELECT FROM information_schema.tables
                                                WHERE table_schema = 'public'
                                                AND table_name = '{table}'
                                                );""")
            exists = cursor.fetchone()[0]
            if not exists:
                cursor.execute(f'CREATE TABLE {table} ({", ".join(tables_mapping[table])});')
                db_connection.commit()

    @staticmethod
    def parse(data, cursor):
        client_info = data[:3]
        specialist_info = data[3:6]
        area_info = data[6:8]
        medical_info = data[8:13]
        record_info = data[13:]
        cursor.execute("SELECT * FROM areas WHERE id=%s", (area_info[0], ))
        same_area = cursor.fetchall()

        cursor.execute("SELECT * FROM clients WHERE id=%s", (client_info[0],))
        same_client = cursor.fetchall()

        cursor.execute("SELECT * FROM specialists WHERE id=%s", (specialist_info[0],))
        same_specialist = cursor.fetchall()

        cursor.execute("SELECT * FROM medical_info WHERE id=%s", (medical_info[0],))
        same_med_info = cursor.fetchall()

        cursor.execute("SELECT * FROM records WHERE id=%s", (record_info[0],))
        same_record = cursor.fetchall()
        try:
            if not same_client:
                sql = """INSERT INTO clients (id, fio, phone) VALUES (%s, %s, %s);"""
                cursor.execute(sql, client_info)

            if not same_area:
                sql = """INSERT INTO areas (id, name) VALUES (%s, %s);"""
                cursor.execute(sql, area_info)

            if not same_specialist:
                sql = """INSERT INTO specialists (id, fio, area_id) VALUES (%s, %s, %s);"""
                cursor.execute(sql, specialist_info)

            if not same_med_info:
                sql = """INSERT INTO medical_info (id, client_id, date, compliant, treatment) VALUES (%s, %s, %s, %s, %s);"""
                cursor.execute(sql, medical_info)

            if not same_record:
                sql = """INSERT INTO records (id, client_id, specialist_id, date, price, medical_info_id) VALUES (%s, %s, %s, %s, %s, %s);"""
                cursor.execute(sql, record_info)

        except Exception as e:
            print(e)
