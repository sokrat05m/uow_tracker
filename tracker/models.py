from dataclasses import dataclass, field
from decimal import Decimal


@dataclass(kw_only=True)
class DomainEntity:
    id: int | None = None

    def __eq__(self, other: object) -> bool:
        if isinstance(other, DomainEntity):
            return bool(other.id == self.id)
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.id)


@dataclass
class OrderLine(DomainEntity):
    product: str
    quantity: int
    price: Decimal

    def change_quantity(self, qty: int):
        self.quantity = qty


@dataclass
class Order(DomainEntity):
    customer: str
    lines: list[OrderLine] = field(default_factory=list)

    def add_line(self, product: str, qty: int, price: Decimal):
        line = OrderLine(
            product=product,
            quantity=qty,
            price=price,
        )
        self.lines.append(line)
        return line
