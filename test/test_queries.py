import typing

from redis.commands.search.query import Query
from sqlalchemy import Delete, Update, and_, delete, select, update

import src.framework.models as models

SELECT_QUERIES_TEST_LIST: list[tuple[tuple[str, Query], typing.Any]] = [
    (("idx:trip", Query("*")), select(models.Trip)),
    (
        ("idx:trip", Query("@passenger_count:[3 inf]")),
        select(models.Trip).where(models.Trip.passenger_count > 2),
    ),
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
    (
        (
            "idx:trip",
            Query(
                "@vendor_name:{Curb*} @trip_distance:[3 inf] @fare_amount:[20 inf]"
            ),
        ),
        select(
            models.Trip.id,
            models.Trip.distance,
            models.Vendor.vendor_name,
            models.Payment.fare_amount,
            models.Fees.airport_fee,
        )
        .join(models.Trip.vendor)
        .join(models.Trip.payment)
        .join(models.Payment.fees)
        .where(
            and_(
                models.Vendor.vendor_name.like("Curb%"),
                models.Trip.distance >= 3,
                models.Payment.fare_amount >= 20,
            )
        )
        .order_by(models.Fees.airport_fee.desc()),
    ),
    # (
    #     (
    #         "idx:trip",
    #         Query("@fare_amount:[50 inf]"),
    #     ),
    #     select(models.Trip)
    #     .join(models.Trip.payment)
    #     .where(
    #         models.Payment.fare_amount
    #         > select(models.Payment.fare_amount)
    #         .select_from(models.Payment)
    #         .limit(1)
    #         .scalar_subquery()
    #     ),
    # ),
]


UPDATE_QUERIES_TEST_LIST: list[
    tuple[
        tuple[tuple[str, Query], dict[str, typing.Any]],
        tuple[Update, dict[str, typing.Any]],
    ]
] = [
    (
        (("idx:trip", Query("@RatecodeID:[2 6]")), {"RatecodeID": 1}),
        (
            update(models.Payment).where(
                and_(
                    models.Payment.rate_code_id >= 2,
                    models.Payment.rate_code_id <= 6,
                )
            ),
            {"rate_code_id": 1},
        ),
    ),
    (
        (
            ("idx:trip", Query("@trip_distance:[-inf 4.99999999]")),
            {"passenger_count": 4},
        ),
        (
            update(models.Trip).where(models.Trip.distance < 5),
            {"passenger_count": 4},
        ),
    ),
    (
        (
            (
                "idx:trip",
                Query(
                    "@trip_distance:[10.00000001 inf] @passenger_count:[1 1]"
                ),
            ),
            {"distance": 9.99, "passenger_count": 2},
        ),
        (
            update(models.Trip).where(
                and_(
                    models.Trip.distance > 10, models.Trip.passenger_count == 1
                )
            ),
            {"distance": 9.99, "passenger_count": 2},
        ),
    ),
    (
        (
            (
                "idx:trip",
                Query(
                    "(@fare_amount:[-inf 4.99999999]) | (@total_amount:[100.00000001 inf])"
                ),
            ),
            {"extra": 0},
        ),
        (
            update(models.Payment).where(
                (models.Payment.fare_amount < 5)
                | (models.Payment.total_amount > 100)
            ),
            {"extra": 0},
        ),
    ),
    (
        (("idx:trip", Query("@trip_distance:[10.00000001 inf]")), {"extra": 5}),
        (
            update(models.Payment).where(
                models.Payment.id.in_(
                    select(models.Trip.payment_id).where(
                        models.Trip.distance > 10
                    )
                )
            ),
            {"extra": 5},
        ),
    ),
    (
        (
            ("idx:trip", Query("@fare_amount:[-inf 4.99999999]")),
            {"Airport_fee": 99},
        ),
        (
            update(models.Fees).where(
                models.Fees.id.in_(
                    select(models.Payment.fees_id).where(
                        models.Payment.fare_amount < 5
                    )
                )
            ),
            {"airport_fee": 99},
        ),
    ),
    (
        (
            ("idx:trip", Query("@passenger_count:[5 5]")),
            {"vendor_name": "Special"},
        ),
        (
            update(models.Vendor).where(
                models.Vendor.id.in_(
                    select(models.Trip.vendor_id).where(
                        models.Trip.passenger_count == 5
                    )
                )
            ),
            {"vendor_name": "Special"},
        ),
    ),
]

DELETE_QUERIES_TEST_LIST: list[tuple[tuple[str, Query], Delete]] = [
    # 1. Delete all trips
    (("idx:trip", Query("*")), delete(models.Trip)),
    # 2. Delete trips with distance < 2
    (
        ("idx:trip", Query("@trip_distance:[-inf 1.99999999]")),
        delete(models.Trip).where(models.Trip.distance < 2),
    ),
    # 3. Delete payments with fare_amount > 100
    (
        ("idx:trip", Query("@fare_amount:[100.00000001 inf]")),
        delete(models.Payment).where(models.Payment.fare_amount > 100),
    ),
    # # 4. Delete vendors not referenced by any Trip record
    # (
    #     ("idx:trip", Query("*")),
    #     delete(models.Vendor).where(
    #         ~models.Vendor.id.in_(select(models.Trip.vendor_id))
    #     ),
    # ),
    # 5. Delete fees where airport_fee = 0 and improvement_surcharge > 1
    (
        (
            "idx:trip",
            Query("@Airport_fee:[0 0] @improvement_surcharge:[1.00000001 inf]"),
        ),
        delete(models.Fees).where(
            (models.Fees.airport_fee == 0)
            & (models.Fees.improvement_surcharge > 1)
        ),
    ),
    # 6. Delete trips where passenger_count is in (2, 3, 5)
    (
        (
            "idx:trip",
            Query(
                "(@passenger_count:[2 2]) | (@passenger_count:[3 3]) | (@passenger_count:[5 5])"
            ),
        ),
        delete(models.Trip).where(models.Trip.passenger_count.in_([2, 3, 5])),
    ),
]
