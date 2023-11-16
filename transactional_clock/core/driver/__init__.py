from transactional_clock.core.storage import ResultingTransaction


class Driver:

    def push(self, resulting: ResultingTransaction, database: str, collection: str): ...

    def generate_id(self): ...
