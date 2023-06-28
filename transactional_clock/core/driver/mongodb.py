import json
import logging

from bson import ObjectId
from pymongo import MongoClient
from pymongo.server_api import ServerApi

from transactional_clock.core.driver import Driver
from transactional_clock.core.storage import ResultingTransaction, TransactionType


class Credentials:

    def __init__(self, username: str, password: str):
        self._username = username
        self._password = password

    @property
    def username(self) -> str:
        return self._username

    @property
    def password(self) -> str:
        return self._password


class DBAccess:

    def __init__(self, src: dict):
        self._addr = src['addr']
        self._port = src['port']
        self._credentials = [Credentials(creds['username'], creds['password']) for creds in src['credentials']]

    def get_credentials(self) -> Credentials:
        creds = self._credentials[-1]
        return creds

    @property
    def address(self) -> str:
        return self._addr

    @property
    def port(self) -> str:
        return self._port


class MongoDBDriver(Driver):

    def __init__(self):
        with open('config.json', 'r') as f:
            txt = f.read()

        config = json.loads(txt)

        accesses = [DBAccess(node) for node in config['nodes']]
        creds = accesses[-1].get_credentials()

        self._instances = list()
        for access in accesses:
            self._instances.append(
                MongoClient(
                    f"mongodb://{creds.username}:{creds.password}@{access.address}:{access.port}/?authSource=admin",
                    server_api=ServerApi('1')
                )
            )

    def push(self, resulting: ResultingTransaction, database: str, collection_name: str):
        logging.debug(resulting.__dict__)
        print(resulting.__dict__)
        if resulting.operation == TransactionType.UPDATE:
            logging.debug('Updating MongoDB...')
            print('Updating MongoDB...')
            for inst in self._instances:
                db = inst[database]
                collection = db[collection_name]

                collection.update_one(
                    {'_id': ObjectId(resulting.id)},
                    {
                        '$set': resulting.data
                    }
                )

            return

        if resulting.operation == TransactionType.CREATE:
            logging.debug('Updating MongoDB...')
            print('Updating MongoDB...')

            insertion = resulting.data
            insertion['_id'] = ObjectId(resulting.id)

            for inst in self._instances:
                db = inst[database]
                collection = db[collection_name]

                collection.insert_one(insertion)

            return

        if resulting.operation == TransactionType.DELETE:
            logging.debug('Deleting from MongoDB...')

            deletion = {'_id': ObjectId(resulting.id)}

            for inst in self._instances:
                db = inst[database]
                collection = db[collection_name]

                collection.delete_one(deletion)

    def generate_id(self) -> ObjectId:
        return ObjectId()
