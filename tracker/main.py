import asyncio
import os
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import sessionmaker

from repo import OrderRepositoryConn
from tracker.mappers import Registry, OrderDataMapper, OrderLineDataMapper
from tracker.prepare_data import random_ids, populate_data
from tracker.repo import OrderRepoSession
from tracker.tables import mapper_registry, map_tables
from uow import UnitOfWork


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    user: str
    password: str
    database: str

    @property
    def url(self) -> str:
        return f"postgresql+psycopg://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


def get_postgres_config() -> PostgresConfig:
    return PostgresConfig(
        host=os.environ.get("POSTGRES_HOST"),
        port=int(os.environ.get("POSTGRES_PORT")),
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD"),
        database=os.environ.get("POSTGRES_DB"),
    )


async def interactor(order_repo: OrderRepositoryConn, uow: UnitOfWork) -> None:
    orders = await order_repo.by_id_in_(order_ids=random_ids)
    for order in orders:
        order.customer = order.customer + "_upd"
    await uow.commit()


async def interactor2(order_repo: OrderRepoSession, session: AsyncSession) -> None:
    orders = await order_repo.by_id_in_(order_ids=random_ids)
    for order in orders:
        order.customer = order.customer + "_upd"
    await session.commit()


async def run_with_custom_mappers():
    config = get_postgres_config()
    engine = create_async_engine(
        url=config.url,
        pool_size=30,
    )
    async with engine.begin() as conn:
        await conn.run_sync(mapper_registry.metadata.create_all)
    async with engine.connect() as connection:
        # await populate_data(connection=connection)
        registry = Registry()
        order_line_mapper = OrderLineDataMapper(connection=connection)
        order_mapper = OrderDataMapper(connection=connection)
        registry.add_mapper(order_line_mapper)
        registry.add_mapper(order_mapper)
        uow = UnitOfWork(connection=connection, mapper_registry=registry)
        order_repository = OrderRepositoryConn(uow=uow, connection=connection)
        before = datetime.now()
        print(f"======TIME BEFORE {before}")
        await interactor(order_repo=order_repository, uow=uow)
        after = datetime.now()
        print(f"======TIME AFTER {after}")
        print(f"======DELTA {after - before}")


async def run_with_sqlalchemy_mappers():
    config = get_postgres_config()
    engine = create_async_engine(
        url=config.url,
        pool_size=30,
    )
    map_tables()
    sm = async_sessionmaker(bind=engine)
    async with sm() as session:
        order_repo = OrderRepoSession(session=session)
        before = datetime.now()
        print(f"======TIME BEFORE {before}")
        await interactor2(order_repo=order_repo, session=session)
        after = datetime.now()
        print(f"======TIME AFTER {after}")
        print(f"======DELTA {after - before}")


asyncio.run(run_with_sqlalchemy_mappers())
