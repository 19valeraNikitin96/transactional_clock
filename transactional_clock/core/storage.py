from datetime import datetime
from enum import Enum


class TransactionType(Enum):
    UPDATE = 1
    DELETE = 2
    CREATE = 3


class TransactionBase:

    def __init__(self, data: dict, operation: TransactionType):
        if data is None and operation != TransactionType.DELETE:
            raise Exception(f"data must be present if operation is {operation}")
        self._data = data
        self._operation = operation

        self.__cleanup()

    def __cleanup(self):
        key = '_id'
        if key in self._data.keys():
            del self._data[key]

    @property
    def data(self) -> dict:
        return self._data

    @property
    def operation(self) -> TransactionType:
        return self._operation


class ResultingTransaction(TransactionBase):

    def __init__(self, _id: str, data: dict, operation: TransactionType, database: str, collection: str):
        super().__init__(data, operation)
        self._id = _id
        self._database = database
        self._collection = collection

    @property
    def id(self) -> str:
        return self._id

    @property
    def database(self) -> str:
        return self._database

    @property
    def collection(self) -> str:
        return self._collection


class Transaction(ResultingTransaction):

    def __init__(self, _id: str, data: dict, created_at: datetime, operation: TransactionType, database: str, collection: str, priority: int):
        super().__init__(_id, data, operation, database, collection)

        if created_at is None and operation == TransactionType.UPDATE:
            raise Exception(f"created_at must be present for {operation}")
        self._created_at = created_at
        self._priority = priority

    @property
    def created_at(self) -> datetime:
        return self._created_at

    @property
    def priority(self) -> int:
        return self._priority

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return self.__str__()


# class TransactionQueue:
#
#     def __init__(self, _id: str, collection_name: str):
#         self._id = _id
#         self._collection_name = collection_name
#         self._reset()
#
#     def add(self, inst: Transaction):
#         self._queue.append(inst)
#         self._queue = sorted(self._queue, key=lambda t: t.created_at)
#
#     def _reset(self):
#         self._queue = list()
#
#     def merge(self) -> ResultingTransaction:
#         if len(self._queue) == 0:
#             return None
#
#         # for t in self._queue:
#         #     if t.operation == TransactionType.DELETE:
#         #         self._reset()
#         #         return t
#
#         result = dict()
#         for t in self._queue:
#             from transactional_clock.utils import dict_merge
#             dict_merge(result, t.data)
#
#         self._reset()
#
#         return ResultingTransaction(self._id, result, TransactionType.UPDATE)
#
#     def __hash__(self):
#         return hash(self._id)
#
#     @property
#     def collection_name(self):
#         return self._collection_name
