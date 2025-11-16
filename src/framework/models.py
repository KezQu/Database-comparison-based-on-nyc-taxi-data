import typing
from datetime import datetime

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class BaseOrmType(DeclarativeBase):
    def to_dict(self) -> dict[str, typing.Any]:
        raise NotImplementedError


class FareRate(BaseOrmType):
    __tablename__ = "fare_rate"

    rate_code_id: Mapped[int] = mapped_column(
        primary_key=True,
        unique=True,
    )
    rate_name: Mapped[str]

    def __repr__(self) -> str:
        return f"FareRate(rate_code_id={self.rate_code_id}, rate_name={self.rate_name})"

    def to_dict(self) -> dict[str, typing.Any]:
        return {
            "rate_code_id": self.rate_code_id,
            "rate_name": self.rate_name,
        }


class TaxiMeter(BaseOrmType):
    __tablename__ = "taxi_meter"

    taxi_meter_id: Mapped[int] = mapped_column(
        primary_key=True,
        unique=True,
        autoincrement=True,
    )
    taxi_meter_date: Mapped[datetime]
    taxi_meter_location: Mapped[str]

    def __repr__(self) -> str:
        return (
            f"TaxiMeter(taxi_meter_id={self.taxi_meter_id}, "
            + f"taxi_meter_date={self.taxi_meter_date}, "
            + f"taxi_meter_location={self.taxi_meter_location})"
        )

    def to_dict(self) -> dict[str, typing.Any]:
        return {
            "taxi_meter_id": self.taxi_meter_id,
            "taxi_meter_date": self.taxi_meter_date,
            "taxi_meter_location": self.taxi_meter_location,
        }


class Vendor(BaseOrmType):
    __tablename__ = "vendor"

    vendor_id: Mapped[int] = mapped_column(
        primary_key=True,
        unique=True,
        autoincrement=True,
    )
    vendor_name: Mapped[str]

    def __repr__(self) -> str:
        return (
            f"Vendor(vendor_id={self.vendor_id}, "
            + f"vendor_name={self.vendor_name})"
        )

    def to_dict(self) -> dict[str, typing.Any]:
        return {
            "vendor_id": self.vendor_id,
            "vendor_name": self.vendor_name,
        }


class Fees(BaseOrmType):
    __tablename__ = "fees"

    fees_id: Mapped[int] = mapped_column(
        primary_key=True,
        unique=True,
        autoincrement=True,
    )
    mta_tax: Mapped[float]
    improvement_surcharge: Mapped[float]
    airport_fee: Mapped[float]
    cbd_congestion_fee: Mapped[float]

    def __repr__(self) -> str:
        return (
            f"Fees(fees_id={self.fees_id}, "
            + f"mta_tax={self.mta_tax}, "
            + f"improvement_surcharge={self.improvement_surcharge}, "
            + f"airport_fee={self.airport_fee}, "
            + f"cbd_congestion_fee={self.cbd_congestion_fee})"
        )

    def to_dict(self) -> dict[str, typing.Any]:
        return {
            "fees_id": self.fees_id,
            "mta_tax": self.mta_tax,
            "improvement_surcharge": self.improvement_surcharge,
            "airport_fee": self.airport_fee,
            "cbd_congestion_fee": self.cbd_congestion_fee,
        }


class Trip(BaseOrmType):
    __tablename__ = "trip"

    trip_id: Mapped[int] = mapped_column(
        primary_key=True,
        unique=True,
        autoincrement=True,
    )
    distance: Mapped[float]
    passenger_count: Mapped[int]
    pickup_id: Mapped[int]
    dropoff_id: Mapped[int]
    payment_id: Mapped[int]
    vendor_id: Mapped[int]

    def __repr__(self) -> str:
        return (
            f"Trip(trip_id={self.trip_id}, "
            + f"distance={self.distance}, "
            + f"passenger_count={self.passenger_count}, "
            + f"pickup_id={self.pickup_id}, "
            + f"dropoff_id={self.dropoff_id}, "
            + f"payment_id={self.payment_id}, "
            + f"vendor_id={self.vendor_id})"
        )

    def to_dict(self) -> dict[str, typing.Any]:
        return {
            "trip_id": self.trip_id,
            "distance": self.distance,
            "passenger_count": self.passenger_count,
            "pickup_id": self.pickup_id,
            "dropoff_id": self.dropoff_id,
            "payment_id": self.payment_id,
            "vendor_id": self.vendor_id,
        }


class Payment(BaseOrmType):
    __tablename__ = "payment"

    payment_id: Mapped[int] = mapped_column(
        primary_key=True,
        unique=True,
        autoincrement=True,
    )
    payment_type: Mapped[float]
    extra: Mapped[float]
    tolls_amount: Mapped[float]
    fare_amount: Mapped[float]
    total_amount: Mapped[float]
    fees_id: Mapped[int]
    rate_code_id: Mapped[int]

    def __repr__(self) -> str:
        return (
            f"Payment(payment_id={self.payment_id}, "
            + f"payment_type={self.payment_type}, "
            + f"extra={self.extra}, "
            + f"tolls_amount={self.tolls_amount}, "
            + f"fare_amount={self.fare_amount}, "
            + f"total_amount={self.total_amount}, "
            + f"fees_id={self.fees_id}, "
            + f"rate_code_id={self.rate_code_id})"
        )

    def to_dict(self) -> dict[str, typing.Any]:
        return {
            "payment_id": self.payment_id,
            "payment_type": self.payment_type,
            "extra": self.extra,
            "tolls_amount": self.tolls_amount,
            "fare_amount": self.fare_amount,
            "total_amount": self.total_amount,
            "fees_id": self.fees_id,
            "rate_code_id": self.rate_code_id,
        }
