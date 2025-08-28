import random
import string

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncConnection

from tracker.tables import ORDER_TABLE, ORDER_LINE_TABLE


def random_string(n=8):
    return "".join(random.choice(string.ascii_uppercase) for _ in range(n))


async def populate_data(
    connection: AsyncConnection,
    n_orders=100_000,
) -> None:
    orders = [{"customer": f"cust_{i}"} for i in range(n_orders)]
    print("=======BEFORE ORDERS")
    result = await connection.execute(
        insert(ORDER_TABLE).returning(ORDER_TABLE.c.id), orders
    )
    print("=======AFTER ORDERS")
    order_ids = [row[0] for row in result]
    lines = [
        {
            "order_id": order_id,
            "product": random_string(),
            "quantity": random.randint(1, 10),
            "price": round(random.uniform(10, 500), 2),
        }
        for order_id in order_ids
    ]
    await connection.execute(insert(ORDER_LINE_TABLE), lines)
    await connection.commit()


random_ids = [
    20023,
    87804,
    9924,
    40782,
    76134,
    42644,
    23555,
    9879,
    14049,
    30059,
    91006,
    86315,
    89303,
    86561,
    88324,
    9570,
    44547,
    92938,
    93462,
    88994,
    37211,
    58972,
    14932,
    41525,
    35035,
    74362,
    37676,
    50780,
    98746,
    73406,
    77610,
    20777,
    7080,
    65636,
    81084,
    15861,
    75395,
    79707,
    62286,
    88177,
    12069,
    2873,
    58740,
    74881,
    47636,
    76173,
    16042,
    65971,
    35596,
    97628,
    44716,
    2583,
    94060,
    84926,
    18627,
    56028,
    27151,
    26139,
    67927,
    8836,
    63459,
    54648,
    69226,
    44986,
    94640,
    28099,
    93274,
    6300,
    52402,
    99384,
    7261,
    98269,
    1545,
    42638,
    20144,
    27689,
    98937,
    83855,
    21605,
    1579,
    53044,
    82876,
    18834,
    69451,
    82224,
    71144,
    79571,
    1573,
    81624,
    8908,
    22525,
    27543,
    40109,
    76309,
    74574,
    2246,
    95396,
    46792,
    22181,
    49914,
]
