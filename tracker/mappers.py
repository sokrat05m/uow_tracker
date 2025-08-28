from typing import Protocol, Iterable

from sqlalchemy import insert, update, bindparam, delete
from sqlalchemy.ext.asyncio import AsyncConnection

from tracker.models import OrderLine, Order, DomainEntity
from tracker.tables import ORDER_LINE_TABLE, ORDER_TABLE


class GenericDataMapper[EntityT: DomainEntity](Protocol):
    async def save(self, entities: Iterable[EntityT]) -> None:
        raise NotImplementedError

    async def update(self, entities: Iterable[EntityT]) -> None:
        raise NotImplementedError

    async def delete(self, entities: Iterable[EntityT]) -> None:
        raise NotImplementedError


class OrderLineDataMapper(GenericDataMapper[OrderLine]):
    def __init__(self, connection: AsyncConnection):
        self._connection = connection

    async def save(self, entities: Iterable[OrderLine]) -> None:
        data_to_insert = [
            {
                "product": entity.product,
                "quantity": entity.quantity,
                "price": entity.price,
            }
            for entity in entities
        ]
        stmt = insert(ORDER_LINE_TABLE)
        await self._connection.execute(stmt, data_to_insert)

    async def update(self, entities: Iterable[OrderLine]) -> None:
        data_to_update = [
            {
                "e_id": entity.id,
                "product": entity.product,
                "quantity": entity.quantity,
                "price": entity.price,
            }
            for entity in entities
        ]
        stmt = (
            update(ORDER_LINE_TABLE)
            .values(
                product=bindparam("product"),
                quantity=bindparam("quantity"),
                price=bindparam("price"),
            )
            .where(ORDER_LINE_TABLE.c.id == bindparam("e_id"))
        )
        await self._connection.execute(stmt, data_to_update)

    async def delete(self, entities: Iterable[OrderLine]) -> None:
        data_to_delete = [entity.id for entity in entities]
        stmt = delete(ORDER_LINE_TABLE).where(ORDER_LINE_TABLE.c.id.in_(data_to_delete))
        await self._connection.execute(stmt)


class OrderDataMapper(GenericDataMapper[Order]):
    def __init__(self, connection: AsyncConnection):
        self._connection = connection

    async def save(self, entities: Iterable[Order]) -> None:
        data_to_insert = [
            {
                "customer": entity.customer,
            }
            for entity in entities
        ]
        stmt = insert(ORDER_TABLE)
        await self._connection.execute(stmt, data_to_insert)

    async def update(self, entities: Iterable[Order]) -> None:
        data_to_update = [
            {
                "e_id": entity.id,
                "customer": entity.customer,
            }
            for entity in entities
        ]
        stmt = (
            update(ORDER_TABLE)
            .values(
                customer=bindparam("customer"),
            )
            .where(ORDER_TABLE.c.id == bindparam("e_id"))
        )
        await self._connection.execute(stmt, data_to_update)

    async def delete(self, entities: Iterable[Order]) -> None:
        data_to_delete = [entity.id for entity in entities]
        stmt = delete(ORDER_TABLE).where(ORDER_TABLE.c.id.in_(data_to_delete))
        await self._connection.execute(stmt)


class Registry:
    def __init__(self):
        self._mappers = {}
        self._keys = {
            OrderLine: OrderLineDataMapper,
            Order: OrderDataMapper,
        }

    def add_mapper(self, mapper):
        self._mappers[type(mapper)] = mapper

    def get(self, entity_type):
        try:
            key = self._keys[entity_type]
        except KeyError as e:
            raise Exception(
                f"The mapper for this entity is not registered {entity_type}"
            ) from e
        try:
            mapper = self._mappers[key]
        except KeyError as e:
            raise Exception("The mapper is not initialized") from e
        return mapper
