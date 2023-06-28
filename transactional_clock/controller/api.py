from datetime import datetime

from fastapi import Request, APIRouter

from transactional_clock.core.service import DEFAULT_PRIORITY, sync_manager, Service, MongoDBFlavor
from transactional_clock.core.storage import Transaction, TransactionType

operations = APIRouter()
service: Service = MongoDBFlavor()


@operations.put("/mongodb")
async def update(request: Request):
    database = request.headers.get('Database')
    collection = request.headers.get('Collection')
    _id = request.headers.get('Id')
    created_at = request.headers.get('CreatedAt')
    priority = request.headers.get('Priority', DEFAULT_PRIORITY)
    priority = int(priority)

    payload = await request.json()

    if priority not in service.unprocessed_queue.keys():
        service.unprocessed_queue[priority] = sync_manager.dict()
        service.order_by_priorities()

    databases: dict = service.unprocessed_queue[priority]
    if database not in databases.keys():
        databases[database] = sync_manager.dict()

    collections: dict = databases[database]
    if collection not in collections.keys():
        collections[collection] = sync_manager.dict()

    ids: dict = collections[collection]
    if _id not in ids.keys():
        ids[_id] = sync_manager.list()

    queue: list = ids[_id]
    t = Transaction(payload, datetime.fromisoformat(created_at), TransactionType.UPDATE)
    queue.append(t)
    ids[_id] = sync_manager.list(sorted(ids[_id], key=lambda t: t.created_at))


@operations.post('/mongodb')
async def create(req: Request):
    database = req.headers.get('Database')
    collection = req.headers.get('Collection')
    priority = req.headers.get('Priority', DEFAULT_PRIORITY)
    priority = int(priority)

    payload = await req.json()

    if priority not in service.unprocessed_queue.keys():
        service.unprocessed_queue[priority] = sync_manager.dict()
        service.order_by_priorities()

    databases: dict = service.unprocessed_queue[priority]
    if database not in databases.keys():
        databases[database] = sync_manager.dict()

    collections: dict = databases[database]
    if collection not in collections.keys():
        collections[collection] = sync_manager.dict()

    ids: dict = collections[collection]
    _id = service.generate_id()
    ids[_id] = sync_manager.list()

    queue: list = ids[_id]
    t = Transaction(payload, None, TransactionType.CREATE)
    queue.append(t)


@operations.delete("/mongodb")
async def update(request: Request):
    database = request.headers.get('Database')
    collection = request.headers.get('Collection')
    _id = request.headers.get('Id')
    priority = request.headers.get('Priority', DEFAULT_PRIORITY)
    priority = int(priority)

    if priority not in service.unprocessed_queue.keys():
        service.unprocessed_queue[priority] = sync_manager.dict()
        service.order_by_priorities()

    databases: dict = service.unprocessed_queue[priority]
    if database not in databases.keys():
        databases[database] = sync_manager.dict()

    collections: dict = databases[database]
    if collection not in collections.keys():
        collections[collection] = sync_manager.dict()

    ids: dict = collections[collection]
    if _id not in ids.keys():
        ids[_id] = sync_manager.list()

    queue: list = ids[_id]
    t = Transaction(None, None, TransactionType.DELETE)
    queue.append(t)

# @operations.delete("/delete")
# async def delete(request: Request):
#     database = request.headers.get('Database')
#     collection = request.headers.get('Collection')
#     _id = request.headers.get('Id')
#     created_at = request.headers.get('CreatedAt')
#     priority = request.headers.get('Priority', DEFAULT_PRIORITY)
#     priority = int(priority)
#
#     mongo_db_service.add_transaction(
#         _id,
#         database,
#         datetime.fromisoformat(created_at),
#         None,
#         collection,
#         priority
#     )

# class CellInfoReq(BaseModel):
#     at_cmd: str
#     timeout: int
# @api.post(f"/config/api/1/modem/at/cmd")
# # def run_AT_cmd(req_body: CellInfoReq):
# def run_AT_cmd(req_body: dict):
#     return req_body

