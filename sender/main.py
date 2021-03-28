import os
import socket
from utils import utils
from Crypto.Cipher import PKCS1_OAEP, AES
from Crypto.PublicKey import RSA
from Crypto.Random import get_random_bytes
import threading
import matplotlib.pyplot as plt
import sqlite3
import pickle
import hashlib
import json


class SocketClient:
    def __init__(self, server_address='127.0.0.1', server_port=8080,
                 header_len_size=64, encoding_format='utf-32-be'):
        self._server_address_ = server_address
        self._server_port_ = server_port
        self._server_ = None
        self._rsa_key_ = None
        self._aes_key_ = get_random_bytes(16)
        self._aes_cypher_ = AES.new(self._aes_key_, AES.MODE_ECB)
        self._HEADER_LEN_SIZE_ = header_len_size
        self._ENCODING_FORMAT_ = encoding_format
        self._db_name_ = 'TRRP2.db'
        self._table_name_ = 'main'

    def _establish_secure_connection_(self):
        conn = tuple((self._server_address_, self._server_port_))
        self._server_ = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_.connect(conn)

        # Получаем n
        n_len = utils.de_pad_message(self._server_.recv(self._HEADER_LEN_SIZE_))
        n_len = int(n_len.decode(self._ENCODING_FORMAT_))
        print(f'[ GETTING N ]')
        n = self._server_.recv(n_len)
        n = int(utils.decompress_data(n).decode(self._ENCODING_FORMAT_))

        # Получаем e
        e_len = utils.de_pad_message(self._server_.recv(self._HEADER_LEN_SIZE_))
        e_len = int(e_len.decode(self._ENCODING_FORMAT_))
        print(f'[ GETTING E ]')
        e = self._server_.recv(e_len)
        e = int(utils.decompress_data(e).decode(self._ENCODING_FORMAT_))

        # шифруем симметричный ключ
        self._rsa_key_ = RSA.construct((n, e))
        print(f'{hashlib.md5(json.dumps(self._rsa_key_, cls=utils.RSAKeyEncoder).encode(self._ENCODING_FORMAT_)).hexdigest()=}')
        rsa_cypher = PKCS1_OAEP.new(self._rsa_key_)
        encrypted_key = rsa_cypher.encrypt(self._aes_key_)
        self._prove_encryption_(encrypted_key)

        # отправляем зашифрованный ключ
        encrypted_key_len = str(len(encrypted_key)).encode(self._ENCODING_FORMAT_)
        self._server_.send(utils.pad_message(encrypted_key_len, self._HEADER_LEN_SIZE_))
        self._server_.send(encrypted_key)

    def _prove_encryption_(self, encrypted_key):
        fig, ax = plt.subplots(2)
        initial_x_dist = sorted([x for x in set(self._aes_key_)])
        initial_y_dist = [self._aes_key_.count(x) for x in initial_x_dist]

        encrypted_x_dist = [x for x in set(encrypted_key)]
        encrypted_y_dist = [encrypted_key.count(x) for x in encrypted_x_dist]

        ax[0].plot(initial_x_dist, initial_y_dist, '.')
        ax[0].title.set_text('Ключ в открытом виде')
        ax[1].plot(encrypted_x_dist, encrypted_y_dist, '.')
        ax[1].title.set_text('Зашифрованный ключ')
        for i in range(2):
            plt.setp(ax[i], xlabel='Значение байта')
            plt.setp(ax[i], ylabel='Количество вхождений')
        plt.tight_layout()
        plt.savefig('encryption_proof.png', dpi=300)

    def start(self):
        print(f'Trying connect to {self._server_address_}:{self._server_port_}')
        self._establish_secure_connection_()
        main_thread = threading.Thread(target=self._main_loop_)
        main_thread.start()

    def _main_loop_(self):
        try:
            data = utils.DataGenerator.get_all_data(self._db_name_, self._table_name_)
        except sqlite3.OperationalError:
            print('[ ERROR ] MISSING SPECIFIED TABLE -- GENERATING NEW ONE [ main ]')
            utils.DataGenerator.generate_db(self._db_name_, self._table_name_)
            utils.DataGenerator.generate_data(100, self._db_name_, self._table_name_)
            data = utils.DataGenerator.get_all_data(self._db_name_, self._table_name_)
        for x in data:
            encoded = pickle.dumps(x)
            compressed = utils.compress_data(encoded)
            print(f'Before encryption {hashlib.md5(compressed).hexdigest()=}')
            encrypted = self._aes_cypher_.encrypt(utils.pad_message(compressed, len(self._aes_key_)))
            print(f'After encryption {hashlib.md5(encrypted).hexdigest()=}')
            x_len = str(len(encrypted)).encode(self._ENCODING_FORMAT_)
            self._server_.send(utils.pad_message(x_len, self._HEADER_LEN_SIZE_))
            self._server_.send(encrypted)
            self._server_.recv(len(b'Ok'))
        bye = pickle.dumps('Disconnect')
        bye = utils.compress_data(bye)
        bye = self._aes_cypher_.encrypt(utils.pad_message(bye, len(self._aes_key_)))
        self._server_.send(str(len(bye)).encode(self._ENCODING_FORMAT_))
        self._server_.send(bye)


if __name__ == '__main__':
    try:
        port = int(os.getenv('port')) or 8080
        host = os.getenv('host') or '127.0.0.1'
        client = SocketClient(host, port)
        client.start()
    except KeyboardInterrupt:
        print('Finishing because client')
    except Exception as e:
        print('Cant connect!')
