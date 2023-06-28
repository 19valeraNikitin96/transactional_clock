import logging
import threading
from multiprocessing import Manager
from queue import Queue

from transactional_clock.core.driver.mongodb import MongoDBDriver
from transactional_clock.core.storage import TransactionType, ResultingTransaction

sync_manager = Manager()

DEFAULT_PRIORITY = 50


class Service:

    def __init__(self):
        self._unprocessed_queue = sync_manager.dict()
        self._resulting_transactions = sync_manager.Queue()

        self._merging_thread = threading.Thread(target=self._merge_transactions, args=())
        logging.debug('Starting merging thread')
        self._merging_thread.start()

    def _merge_transactions(self):
        while True:
            _sorted = sorted(self._unprocessed_queue.items())
            logging.debug(f"Sorted: {_sorted}")
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

                            res = dict()
                            for t in transactions:
                                from transactional_clock.core.util import dict_merge
                                dict_merge(res, t.data)

                            res = ResultingTransaction(_id, res, TransactionType.UPDATE, database, collection)
                            self._resulting_transactions.put(res)
                            ids[_id] = sync_manager.list()

    def _push_resulting_transactions(self): ...

    @property
    def unprocessed_queue(self) -> dict:
        return self._unprocessed_queue

    @property
    def resulting_transactions(self) -> Queue:
        return self._resulting_transactions

    def order_by_priorities(self):
        lock = sync_manager.Lock()
        lock.acquire()
        keys = list(self._unprocessed_queue.keys())
        keys.sort()
        self._unprocessed_queue = sync_manager.dict(
            {k: self._unprocessed_queue[k] for k in keys}
        )
        lock.release()


class MongoDBFlavor(Service):

    def __init__(self):
        super().__init__()
        self._driver = MongoDBDriver()
        self._pushing_thread = threading.Thread(target=self._push_resulting_transactions, args=())
        logging.debug('Starting pushing thread')
        self._pushing_thread.start()

    def _push_resulting_transactions(self):
        while True:
            while self._resulting_transactions.qsize() != 0:
                print(f"Resulting transactions: {self._resulting_transactions.qsize()}")
                t: ResultingTransaction = self._resulting_transactions.get()
                self._driver.push(t, t.database, t.collection)
