import logging
import threading
from multiprocessing import Manager
from queue import Queue

from transactional_clock.core.driver.mongodb import MongoDBDriver
from transactional_clock.core.storage import TransactionType, ResultingTransaction, Transaction

sync_manager = Manager()
sync_lock = sync_manager.Lock()

DEFAULT_PRIORITY = 50


class Service:

    def __init__(self):
        self._unprocessed_transactions = sync_manager.Queue() # put any ControllerTransaction
        self._transactions = sync_manager.dict() # processed transactions
        self._resulting_transactions = sync_manager.Queue() # resulting transactions

        self._hosting_thread = threading.Thread(target=self._add_newcomes, args=())
        logging.debug('Starting hosting thread...')
        self._hosting_thread.start()
        self._merging_thread = threading.Thread(target=self._merge_transactions, args=())
        logging.debug('Starting merging thread...')
        self._merging_thread.start()

    def add_new_transaction(self, t: Transaction):
        sync_lock.acquire()
        self._unprocessed_transactions.put(t)
        print(f"Adding: {t}")
        sync_lock.release()

    def _add_newcomes(self):
        while True:
            sync_lock.acquire()
            while self._unprocessed_transactions.qsize() != 0:

                t = self._unprocessed_transactions.get()

                if t.priority not in self._transactions.keys():
                    self._transactions[t.priority] = sync_manager.dict()
                    self.order_by_priorities()

                databases: dict = self._transactions[t.priority]
                if t.database not in databases.keys():
                    databases[t.database] = sync_manager.dict()

                collections: dict = databases[t.database]
                if t.collection not in collections.keys():
                    collections[t.collection] = sync_manager.dict()

                ids: dict = collections[t.collection]
                if t.id not in ids.keys():
                    ids[t.id] = sync_manager.list()

                queue: list = ids[t.id]
                queue.append(t)
                ids[t.id] = sync_manager.list(sorted(ids[t.id], key=lambda t: t.created_at))

            sync_lock.release()

    def _merge_transactions(self):
        while True:
            _sorted = sorted(self._transactions.items())
            logging.debug(f"Sorted: {_sorted}")
            sync_lock.acquire()
            for priority, databases in _sorted:
                logging.debug(f"Databases: {databases}")
                for database, collections in databases.items():
                    for collection, ids in collections.items():
                        for _id, transactions in ids.items():
                            if len(transactions) == 0:
                                continue

                            deletes = [t for t in transactions if t.operation == TransactionType.DELETE]
                            if len(deletes) > 0:
                                res = ResultingTransaction(_id, None, TransactionType.DELETE, database, collection)
                                self._resulting_transactions.put(res)
                                ids[_id] = sync_manager.list()
                                continue

                            creates = [t for t in transactions if t.operation == TransactionType.CREATE]
                            if len(creates) > 0:
                                t = creates[-1]
                                res = ResultingTransaction(_id, t.data, TransactionType.CREATE, database, collection)
                                self._resulting_transactions.put(res)

                            updates = [t for t in transactions if t.operation == TransactionType.UPDATE]
                            if len(updates) == 0:
                                ids[_id] = sync_manager.list()
                                continue

                            res = dict()
                            for t in updates:
                                from transactional_clock.core.util import dict_merge
                                dict_merge(res, t.data)

                            res = ResultingTransaction(_id, res, TransactionType.UPDATE, database, collection)
                            self._resulting_transactions.put(res)
                            ids[_id] = sync_manager.list()

            sync_lock.release()

    def _push_resulting_transactions(self): ...

    @property
    def unprocessed_transactions(self) -> Queue[Transaction]:
        return self._unprocessed_transactions

    @property
    def transactions(self) -> dict:
        return self._transactions

    @property
    def resulting_transactions(self) -> Queue:
        return self._resulting_transactions

    def order_by_priorities(self):
        keys = list(self._transactions.keys())
        keys.sort()
        self._transactions = sync_manager.dict(
            {k: self._transactions[k] for k in keys}
        )

    def generate_id(self): ...


class MongoDBFlavor(Service):

    def __init__(self):
        super().__init__()
        self._driver = MongoDBDriver()
        self._pushing_thread = threading.Thread(target=self._push_resulting_transactions, args=())
        logging.debug('Starting pushing thread...')
        self._pushing_thread.start()

    def _push_resulting_transactions(self):
        while True:
            sync_lock.acquire()
            while self._resulting_transactions.qsize() != 0:
                print(f"Resulting transactions: {self._resulting_transactions.qsize()}")
                t: ResultingTransaction = self._resulting_transactions.get()
                self._driver.push(t, t.database, t.collection)
            sync_lock.release()

    def generate_id(self) -> str:
        return str(self._driver.generate_id())
