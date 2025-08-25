import copy
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from functools import wraps
from uuid import uuid4


@dataclass
class DomainEntity:
    id: str


@dataclass
class Product(DomainEntity):
    name: str


@dataclass
class OrderLine(DomainEntity):
    product: Product
    quantity: int
    price: float

    def change_quantity(self, qty: int):
        self.quantity = qty


@dataclass
class Order(DomainEntity):
    customer: str
    integers: list[int]
    lines: list[OrderLine] = field(default_factory=list)

    def add_line(self, product: str, qty: int, price: float):
        line = OrderLine(
            id=str(uuid4()),
            product=Product("7", name=product),
            quantity=qty,
            price=price,
        )
        self.lines.append(line)
        return line


class EntityChangeTracker:
    def __init__(self):
        self.new_entities = []
        self.modified_entities = []
        self.deleted_entities = []
        self.entity_snapshots = {}

    def register_new(self, entity: DomainEntity) -> None:
        self.new_entities.append(entity)

    def take_snapshot(self, entity: DomainEntity) -> None:
        self.entity_snapshots[(type(entity), entity.id)] = (
            entity,
            copy.deepcopy(entity.__dict__),
        )
        for value in entity.__dict__.values():
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, DomainEntity):
                        self.take_snapshot(item)
            elif isinstance(value, DomainEntity):
                self.take_snapshot(value)

    def collect_changes(self, entity: DomainEntity) -> None:
        _, orig_state = self.entity_snapshots[(type(entity), entity.id)]
        curr_state = copy.deepcopy(entity.__dict__)

        for attr_name, curr_value in curr_state.items():
            orig_value = orig_state[attr_name]
            self.compare_values(orig_value, curr_value, entity)

    def compare_values(self, orig, curr, parent: DomainEntity) -> None:
        if isinstance(curr, DomainEntity):
            self.collect_changes(curr)
        elif isinstance(curr, list):
            for o, c in zip(orig, curr):
                if isinstance(o, DomainEntity):
                    self.collect_changes(c)
                else:
                    if orig != curr and parent not in self.modified_entities:
                        self.modified_entities.append(parent)
        else:
            if orig != curr and parent not in self.modified_entities:
                self.modified_entities.append(parent)


class UnitOfWork:
    def __init__(self):
        self.change_tracker = EntityChangeTracker()

    def register_new(self, entity: DomainEntity) -> None:
        self.change_tracker.register_new(entity)

    def register_existing(self, entity: DomainEntity) -> None:
        self.change_tracker.take_snapshot(entity)

    def register_deleted(self, entity):
        self.change_tracker.deleted_entities.append(entity)

    def commit(self):
        for entity, _ in self.change_tracker.entity_snapshots.values():
            self.change_tracker.collect_changes(entity)

        for e in self.change_tracker.new_entities:
            print(f"INSERT {e}")
        for e in self.change_tracker.modified_entities:
            print(f"UPDATE {e}")
        for e in self.change_tracker.deleted_entities:
            print(f"DELETE {e}")

        self.change_tracker = EntityChangeTracker()


def track(func: Callable):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        result = func(self, *args, **kwargs)
        if result is None:
            return
        if type(result) is Iterable:
            for entity in result:
                self.uow.register_existing(entity)
        else:
            self.uow.register_existing(result)
        return result

    return wrapper


class OrderRepository:
    def __init__(self, uow: UnitOfWork):
        self.uow = uow
        self.orders = [
            Order(
                id="1",
                customer="C1",
                lines=[OrderLine(id="10", product=Product(id="1", name="P1"), price=15, quantity=10)], integers=[1, 2]
            ),
            Order(
                id="2",
                customer="C2",
                lines=[OrderLine(id="12", product=Product(id="2", name="P2"), price=20, quantity=5)], integers=[3, 4]
            ),
        ]

    @track
    def by_id(self, order_id: str) -> Order | None:
        return next((o for o in self.orders if o.id == order_id), None)


uow = UnitOfWork()
order_repo = OrderRepository(uow=uow)
order = order_repo.by_id(order_id="2")
order.customer = "C3"
uow.commit()