import typing
from datetime import datetime

from sqlalchemy import ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class BaseOrmType(DeclarativeBase):
    def to_dict(self) -> dict[str, typing.Any]:
        raise NotImplementedError


class FareRate(BaseOrmType):
    __tablename__ = "fare_rate"

    id: Mapped[int] = mapped_column(
        primary_key=True,
        unique=True,
    )
    rate_name: Mapped[str]

    def __repr__(self) -> str:
        return f"FareRate({self.to_dict()})"

    def to_dict(self) -> dict[str, typing.Any]:
        return {
            "id": self.id,
            "rate_name": self.rate_name,
        }


class TaxiMeter(BaseOrmType):
    __tablename__ = "taxi_meter"

    id: Mapped[int] = mapped_column(
        primary_key=True,
        unique=True,
        autoincrement=True,
    )
    taxi_meter_date: Mapped[datetime]
    taxi_meter_location: Mapped[str]

    def __repr__(self) -> str:
        return f"TaxiMeter({self.to_dict()})"

    def to_dict(self) -> dict[str, typing.Any]:
        return {
            "id": self.id,
            "taxi_meter_date": self.taxi_meter_date,
            "taxi_meter_location": self.taxi_meter_location,
        }


class Vendor(BaseOrmType):
    __tablename__ = "vendor"

    id: Mapped[int] = mapped_column(
        primary_key=True,
        unique=True,
        autoincrement=True,
    )
    vendor_name: Mapped[str]

    def __repr__(self) -> str:
        return f"Vendor({self.to_dict()})"

    def to_dict(self) -> dict[str, typing.Any]:
        return {
            "id": self.id,
            "vendor_name": self.vendor_name,
        }


class Fees(BaseOrmType):
    __tablename__ = "fees"

    id: Mapped[int] = mapped_column(
        primary_key=True,
        unique=True,
        autoincrement=True,
    )
    mta_tax: Mapped[float]
    improvement_surcharge: Mapped[float]
    airport_fee: Mapped[float]
    cbd_congestion_fee: Mapped[float]

    def __repr__(self) -> str:
        return f"Fees({self.to_dict()})"

    def to_dict(self) -> dict[str, typing.Any]:
        return {
            "id": self.id,
            "mta_tax": self.mta_tax,
            "improvement_surcharge": self.improvement_surcharge,
            "airport_fee": self.airport_fee,
            "cbd_congestion_fee": self.cbd_congestion_fee,
        }


class Payment(BaseOrmType):
    __tablename__ = "payment"

    id: Mapped[int] = mapped_column(
        primary_key=True,
        unique=True,
        autoincrement=True,
    )
    payment_type: Mapped[float]
    extra: Mapped[float]
    tolls_amount: Mapped[float]
    fare_amount: Mapped[float]
    total_amount: Mapped[float]
    fees_id: Mapped[int] = mapped_column(ForeignKey("fees.id"))
    rate_code_id: Mapped[int] = mapped_column(ForeignKey("fare_rate.id"))
    fees: Mapped[Fees] = relationship("Fees")
    rate_code: Mapped[FareRate] = relationship("FareRate")

    def __repr__(self) -> str:
        return f"Payment({self.to_dict()})"

    def to_dict(self) -> dict[str, typing.Any]:
        return {
            "id": self.id,
            "payment_type": self.payment_type,
            "extra": self.extra,
            "tolls_amount": self.tolls_amount,
            "fare_amount": self.fare_amount,
            "total_amount": self.total_amount,
            "fees": self.fees.to_dict() if self.fees else self.fees_id,
            "rate_code": self.rate_code.to_dict()
            if self.rate_code
            else self.rate_code_id,
        }


class Trip(BaseOrmType):
    __tablename__ = "trip"

    id: Mapped[int] = mapped_column(
        primary_key=True,
        unique=True,
        autoincrement=True,
    )
    distance: Mapped[float]
    passenger_count: Mapped[int]
    pickup_id: Mapped[int] = mapped_column(ForeignKey("taxi_meter.id"))
    dropoff_id: Mapped[int] = mapped_column(ForeignKey("taxi_meter.id"))
    payment_id: Mapped[int] = mapped_column(ForeignKey("payment.id"))
    vendor_id: Mapped[int] = mapped_column(ForeignKey("vendor.id"))
    pickup: Mapped[TaxiMeter] = relationship(
        "TaxiMeter", foreign_keys=[pickup_id]
    )
    dropoff: Mapped[TaxiMeter] = relationship(
        "TaxiMeter", foreign_keys=[dropoff_id]
    )
    payment: Mapped[Payment] = relationship("Payment")
    vendor: Mapped[Vendor] = relationship("Vendor")

    def __repr__(self) -> str:
        return f"Trip({self.to_dict()})"

    def to_dict(self) -> dict[str, typing.Any]:
        return {
            "id": self.id,
            "distance": self.distance,
            "passenger_count": self.passenger_count,
            "pickup": self.pickup.to_dict() if self.pickup else self.pickup_id,
            "dropoff": self.dropoff.to_dict()
            if self.dropoff
            else self.dropoff_id,
            "payment": self.payment.to_dict()
            if self.payment
            else self.payment_id,
            "vendor": self.vendor.to_dict() if self.vendor else self.vendor_id,
        }
