import typing

from redis.commands.search.query import Query
from sqlalchemy import and_, select

import src.framework.models as models

SELECT_QUERIES_TEST_LIST: list[tuple[tuple[str, Query], typing.Any]] = [
    # 1. All trips
    (("idx:trip", Query("*")), select(models.Trip)),
    # 2. Trips with more than 2 passengers
    (
        ("idx:trip", Query("@passenger_count:[3 inf]")),
        select(models.Trip).where(models.Trip.passenger_count > 2),
    ),
    # 3. Trips with RatecodeID == 1 and distance > 5
    (
        (
            "idx:trip",
            Query("@RatecodeID:[1 1] @trip_distance:[5.00000001 inf]"),
        ),
        select(models.Trip)
        .join(models.Trip.payment)
        .where(
            and_(models.Payment.rate_code_id == 1, models.Trip.distance > 5)
        ),
    ),
    # 4. Trips with airport_fee > 0, trip_distance < 2, fare_amount > 10, passenger_count >= 2 and not 4, group by and order by
    (
        (
            "idx:trip",
            Query(
                "@Airport_fee:[0.00000001 inf] @trip_distance:[-inf 1.99999999] "
                "@fare_amount:[10.00000001 inf] (@passenger_count:[1 3] | @passenger_count:[5 5])"
            ),
        ),
        select(
            models.Trip,
        )
        .join(models.Trip.payment)
        .join(models.Payment.fees)
        .where(
            and_(
                models.Trip.distance < 2,
                models.Fees.airport_fee > 0,
                models.Payment.fare_amount > 10,
                models.Trip.passenger_count.in_([1, 2, 3, 5]),
            )
        )
        .group_by(
            models.Fees.airport_fee,
            models.Payment.fare_amount,
            models.Payment.total_amount,
            models.Trip.id,
            models.Trip.distance,
            models.Fees.id,
            models.Payment.id,
        )
        .order_by(models.Payment.total_amount.desc()),
    ),
]
