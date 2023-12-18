import logging
import threading
from multiprocessing import Lock
from queue import Queue

from transactional_clock.core.driver.mongodb import MongoDBDriver
from transactional_clock.core.storage import TransactionType, ResultingTransaction, Transaction

sync_lock = Lock()

DEFAULT_PRIORITY = 50


class Service:

    def __init__(self):
        self._unprocessed_transactions = Queue() # put any transactions
        self._transactions = dict() # processed transactions
        self._resulting_transactions = Queue() # resulting transactions

        self._is_paused = False

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

    def _reset(self):
        sync_lock.acquire()
        self._unprocessed_transactions = Queue()
        self._transactions = dict()
        self._resulting_transactions = Queue()
        sync_lock.release()

    def on(self):
        sync_lock.acquire()
        self._is_paused = False
        sync_lock.release()

    def off(self):
        sync_lock.acquire()
        self._is_paused = True
        sync_lock.release()

    def _add_newcomes(self):
        while True:
            sync_lock.acquire()
            while self._unprocessed_transactions.qsize() != 0:

                t = self._unprocessed_transactions.get()

                if t.priority not in self._transactions.keys():
                    self._transactions[t.priority] = dict()
                    self.order_by_priorities()

                databases: dict = self._transactions[t.priority]
                if t.database not in databases.keys():
                    databases[t.database] = dict()

                collections: dict = databases[t.database]
                if t.collection not in collections.keys():
                    collections[t.collection] = dict()

                ids: dict = collections[t.collection]
                if t.id not in ids.keys():
                    ids[t.id] = list()

                queue: list = ids[t.id]
                queue.append(t)
                ids[t.id] = list(sorted(ids[t.id], key=lambda t: t.created_at))

            sync_lock.release()

    def _merge_transactions(self):
        def do_iteration():
            _sorted = sorted(self._transactions.items())
            logging.debug(f"Sorted: {_sorted}")

            is_processed = False
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
                                ids[_id] = list()
                                continue

                            creates = [t for t in transactions if t.operation == TransactionType.CREATE]
                            if len(creates) > 0:
                                t = creates[-1]
                                res = ResultingTransaction(_id, t.data, TransactionType.CREATE, database, collection)
                                self._resulting_transactions.put(res)

                            updates = [t for t in transactions if t.operation == TransactionType.UPDATE]
                            if len(updates) == 0:
                                ids[_id] = list()
                                continue

                            res = dict()
                            for t in updates:
                                from transactional_clock.core.util import dict_merge
                                dict_merge(res, t.data)

                            res = ResultingTransaction(_id, res, TransactionType.UPDATE, database, collection)
                            self._resulting_transactions.put(res)
                            ids[_id] = list()

                            is_processed = True

                if is_processed:
                    break

        while True:
            if self._is_paused:
                continue

            sync_lock.acquire()
            do_iteration()
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
        self._transactions = {k: self._transactions[k] for k in keys}

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
