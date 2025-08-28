from sqlalchemy import (
    MetaData,
    Table,
    Column,
    BigInteger,
    String,
    Integer,
    Numeric,
    ForeignKey,
)
from sqlalchemy.orm import registry, relationship

from tracker.models import OrderLine, Order

convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

metadata = MetaData(naming_convention=convention)
mapper_registry = registry(metadata=metadata)


ORDER_LINE_TABLE = Table(
    "order_lines",
    mapper_registry.metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("order_id", ForeignKey("orders.id")),
    Column("product", String),
    Column("quantity", Integer),
    Column("price", Numeric(15, 2)),
)

ORDER_TABLE = Table(
    "orders",
    mapper_registry.metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("customer", String),
)


def map_tables() -> None:
    mapper_registry.map_imperatively(
        OrderLine,
        ORDER_LINE_TABLE,
    )
    mapper_registry.map_imperatively(
        Order,
        ORDER_TABLE,
        properties={
            "interfaces": relationship(
                OrderLine,
                lazy="joined",
                cascade="all, delete-orphan",
                collection_class=list,
            )
        },
    )
