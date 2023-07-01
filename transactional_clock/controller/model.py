# from datetime import datetime
#
# from transactional_clock.core.storage import TransactionBase, TransactionType
#
#
# class ControllerTransaction(TransactionBase):
#
#     def __init__(self, _id: str, data: dict, operation: TransactionType, created_at: datetime, database: str, collection: str, priority: int):
#         super().__init__(data, operation)
#         self._id = _id
#         self._created_at = created_at
#         self._database = database
#         self._collection = collection
#         self._priority = priority
#
#     @property
#     def id(self) -> str:
#         return self._id
#
#     @property
#     def created_at(self) -> datetime:
#         return self._created_at
#
#     @property
#     def database(self) -> str:
#         return self._database
#
#     @property
#     def collection(self) -> str:
#         return self._collection
#
#     @property
#     def priority(self) -> int:
#         return self._priority
