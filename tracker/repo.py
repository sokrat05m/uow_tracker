from collections.abc import Awaitable
from functools import wraps
from typing import Callable, Iterable, Sequence, Any

from sqlalchemy import select, CursorResult, Row
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from tracker.models import Order, OrderLine
from tracker.tables import ORDER_TABLE, ORDER_LINE_TABLE
from uow import UnitOfWork


def track(func: Callable[..., Awaitable[Any]]):
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        result = await func(self, *args, **kwargs)

        if result is None:
            return None

        if isinstance(result, Iterable) and not isinstance(result, (str, bytes)):
            for entity in result:
                self._uow.register_existing(entity)
        else:
            self._uow.register_existing(result)

        return result

    return wrapper


class OrderRepositoryConn:
    def __init__(self, uow: UnitOfWork, connection: AsyncConnection):
        self._uow = uow
        self._connection = connection

    def _build_orders(self, rows: Sequence[Row]) -> list[Order]:
        orders_map: dict[int, Order] = {}

        for row in rows:
            order = orders_map.get(row.order_id)
            if not order:
                order = Order(
                    id=row.order_id,
                    customer=row.customer,
                    lines=[],
                )
                orders_map[row.order_id] = order

            if row.order_line_id:
                order.lines.append(
                    OrderLine(
                        product=row.product,
                        quantity=row.quantity,
                        price=row.price,
                    )
                )

        return list(orders_map.values())

    def _load_one(self, result: CursorResult) -> Order | None:
        rows = result.all()
        if not rows:
            return None
        return self._build_orders(rows)[0]

    def _load_many(self, result: CursorResult) -> list[Order]:
        rows = result.all()
        if not rows:
            return []
        return self._build_orders(rows)

    @track
    async def by_id(self, order_id: int) -> Order | None:
        stmt = (
            select(
                ORDER_TABLE.c.id.label("order_id"),
                ORDER_TABLE.c.customer.label("customer"),
                ORDER_LINE_TABLE.c.id.label("order_line_id"),
                ORDER_LINE_TABLE.c.product.label("product"),
                ORDER_LINE_TABLE.c.quantity.label("quantity"),
                ORDER_LINE_TABLE.c.price.label("price"),
            )
            .select_from(ORDER_TABLE.join(ORDER_LINE_TABLE, isouter=True))
            .where(ORDER_TABLE.c.id == order_id)
        )
        result = await self._connection.execute(stmt)
        return self._load_one(result=result)

    @track
    async def by_id_in_(self, order_ids: list[int]) -> list[Order]:
        if not order_ids:
            return []

        stmt = (
            select(
                ORDER_TABLE.c.id.label("order_id"),
                ORDER_TABLE.c.customer.label("customer"),
                ORDER_LINE_TABLE.c.id.label("order_line_id"),
                ORDER_LINE_TABLE.c.product.label("product"),
                ORDER_LINE_TABLE.c.quantity.label("quantity"),
                ORDER_LINE_TABLE.c.price.label("price"),
            )
            .select_from(ORDER_TABLE.join(ORDER_LINE_TABLE, isouter=True))
            .where(ORDER_TABLE.c.id.in_(order_ids))
            .order_by(ORDER_TABLE.c.id)
        )
        result = await self._connection.execute(stmt)
        return self._load_many(result=result)


class OrderRepoSession:
    def __init__(self, session: AsyncSession):
        self._session = session

    async def by_id_in_(self, order_ids: list[int]) -> list[Order]:
        stmt = select(Order).where(ORDER_TABLE.c.id.in_(order_ids))
        result = await self._session.execute(stmt)
        return list(result.unique().scalars().all())
