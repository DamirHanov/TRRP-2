import threading
from utils import utils
import socket
from time import gmtime, strftime
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP, AES
import os
import pickle
import psycopg2
import hashlib
import urllib.parse
import json


class TCPServer:
    def __init__(self, host, port, db_connection):
        self._host_ = host
        self._port_ = port

        self._socket_ = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket_.bind((host, port))
        self._HEADER_LEN_SIZE_ = 64
        self._RSA_BITS_COUNT_ = 3072
        self._MAX_ENCRYPTION_SIZE_ = self._RSA_BITS_COUNT_ // 8 - 48
        self._ENCODING_FORMAT_ = 'utf-32-be'
        result = urllib.parse.urlparse(db_connection)
        username = result.username
        password = result.password
        database = result.path[1:]
        hostname = result.hostname
        db_port = result.port
        self._db_connection_ = psycopg2.connect(dbname=database, user=username, port=db_port, password=password,
                                                host=hostname)
        utils.DataParser.check_db(self._db_connection_)

    def start_serving(self):
        print(f'Start listening on {self._host_}:{self._port_}')
        self._socket_.listen()

        while True:
            connection, address = self._socket_.accept()
            client_thread = threading.Thread(target=self._handle_client_, args=(address, connection))
            client_thread.start()

    def _handle_client_(self, address, connection):
        print(f'[ {strftime("%Y-%m-%d %H:%M:%S", gmtime())} ] - NEW CONNECTION FROM {address[0]}:{address[1]}')
        connection.recv(0)
        cypher = self._establish_secure_connection_(connection, address)

        while True:
            try:
                data_len = utils.de_pad_message(connection.recv(self._HEADER_LEN_SIZE_))
                data_len = int(data_len.decode(self._ENCODING_FORMAT_))
                data = connection.recv(data_len)
                print(f'Before decryption {hashlib.md5(data).hexdigest()=}')
                decrypted = utils.de_pad_message(cypher.decrypt(data))
                print(f'After decryption {hashlib.md5(decrypted).hexdigest()=}')
                decompressed = utils.decompress_data(decrypted)
                real_data = pickle.loads(decompressed)
                if real_data == 'Disconnect':
                    print(f'[ {strftime("%Y-%m-%d %H:%M:%S", gmtime())} ] - DROPPING CONNECTION FROM {address[0]}:{address[1]}')
                    break
                utils.DataParser.parse(real_data, self._db_connection_.cursor())
                self._db_connection_.commit()
                connection.send(b'Ok')
            except ValueError:
                print(f'[ {strftime("%Y-%m-%d %H:%M:%S", gmtime())} ] - ERROR -', end=' ')
                print(f'MOST LIKELY GOT CORRUPTED DATA_LEN OR CLIENT [ {address[0]}:{address[1]} ] FINISHED WORK '
                      f'UNEXPECTEDLY -', end=' ')
                print('DROPPING CONNECTION')
                break

    def _establish_secure_connection_(self, connection, address):
        # генерируем пару RSA ключей
        print(f'[ {strftime("%Y-%m-%d %H:%M:%S", gmtime())} ] - GENERATING RSA KEYS FOR {address[0]}:{address[1]}')
        private_key = RSA.generate(self._RSA_BITS_COUNT_)
        public_key = private_key.public_key()
        print(f'{hashlib.md5(json.dumps(private_key, cls=utils.RSAKeyEncoder).encode(self._ENCODING_FORMAT_)).hexdigest()=}')
        print(f'{hashlib.md5(json.dumps(public_key, cls=utils.RSAKeyEncoder).encode(self._ENCODING_FORMAT_)).hexdigest()=}')
        # получаем байты
        public_key_n = str(public_key.n).encode(self._ENCODING_FORMAT_)
        public_key_e = str(public_key.e).encode(self._ENCODING_FORMAT_)
        print(f'[ {address[0]}:{address[1]} ] - Initial {len(public_key_n)=}')
        print(f'[ {address[0]}:{address[1]} ] - Initial {len(public_key_e)=}')

        # сжимаем
        public_key_n = utils.compress_data(public_key_n)
        public_key_e = utils.compress_data(public_key_e)
        print(f'[ {address[0]}:{address[1]} ] - Compressed {len(public_key_n)=}')
        print(f'[ {address[0]}:{address[1]} ] - Compressed {len(public_key_e)=}')

        # отсылаем открытый ключ и принимаем зашифрованный симметричный ключ
        try:
            print(f'[ {strftime("%Y-%m-%d %H:%M:%S", gmtime())} ] - SHARING KEY FOR {address[0]}:{address[1]}')
            public_key_n_len = str(len(public_key_n)).encode(self._ENCODING_FORMAT_)
            connection.send(utils.pad_message(public_key_n_len, self._HEADER_LEN_SIZE_))
            connection.send(public_key_n)

            public_key_e_len = str(len(public_key_e)).encode(self._ENCODING_FORMAT_)
            connection.send(utils.pad_message(public_key_e_len, self._HEADER_LEN_SIZE_))
            connection.send(public_key_e)

            aes_key_len = utils.de_pad_message(connection.recv(self._HEADER_LEN_SIZE_))
            aes_key_len = int(aes_key_len.decode(self._ENCODING_FORMAT_))
            aes_key = connection.recv(aes_key_len)
            print(f'[ {strftime("%Y-%m-%d %H:%M:%S", gmtime())} ] - GOT ENCRYPTED SYMMETRIC KEY FROM {address[0]}:{address[1]}')
            rsa_cypher = PKCS1_OAEP.new(private_key)

            # дешифруем ключ
            aes_key = rsa_cypher.decrypt(aes_key)
            aes_cypher = AES.new(aes_key, AES.MODE_ECB)
            print(f'[ {strftime("%Y-%m-%d %H:%M:%S", gmtime())} ] - PREPARED EVERYTHING FOR SECURE COMMUNICATION WITH {address[0]}:{address[1]}')
        except ConnectionResetError or ValueError:
            print(f'[ {strftime("%Y-%m-%d %H:%M:%S", gmtime())} ] - DROPPING CONNECTION FROM {address[0]}:{address[1]}')
            return

        # возвращаем настроенный объект для шифрования
        return aes_cypher


if __name__ == '__main__':
    conn = os.getenv('db_conn') or 'postgresql://localhost:5432/trrp2'
    host = os.getenv('host') or '0.0.0.0'
    port = os.getenv('port') or 8080
    server = TCPServer(host, port, conn)
    server.start_serving()
