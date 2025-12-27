import logging
import typing

import pandas as pd
from redis.commands.search.field import Field, NumericField, TextField
from redis.commands.search.index_definition import IndexDefinition, IndexType

from .framework import models
from .framework.abstract_database import AbstractDatabase
from .framework.crud_handlers import OrmCRUDHandler, RedisCRUDHandler

ORM_TABLE_TYPE = typing.TypeVar("ORM_TABLE_TYPE", bound=models.BaseOrmType)


def InsertRecordIntoDatabase(
    orm_type: typing.Type[ORM_TABLE_TYPE],
    database: AbstractDatabase,
    **kwargs: str,
) -> typing.Optional[int]:
    orm_handler = OrmCRUDHandler(database.GetDatabaseEngine())

    try:
        return orm_handler.create(orm_type, **kwargs)
    except Exception:
        pass
    return None


def MapRateCodeIdToName(rate_code_id: int) -> str:
    rate_code_mapping = {
        1: "Standard rate",
        2: "JFK",
        3: "Newark",
        4: "Nassau or Westchester",
        5: "Negotiated fare",
        6: "Group ride",
        99: "Null",
    }
    return rate_code_mapping[rate_code_id]


def MapVendorIdToName(vendor_id: int) -> str:
    vendor_mapping = {
        1: "Creative Mobile Technologies, LLC",
        2: "Curb Mobility, LLC",
        6: "Myle Technologies Inc",
        7: "Helix",
    }
    return vendor_mapping[vendor_id]


def CalcPercentage(current: int, total: int) -> float:
    return (current / total) * 100.0 if total > 0 else 0.0


def LoadNycTaxiDataToSqlDatabase(
    database: AbstractDatabase, taxi_data: pd.DataFrame
) -> None:
    total_rows = len(taxi_data)
    for row_id, row in taxi_data.iterrows():
        InsertRecordIntoDatabase(
            models.FareRate,
            database,
            id=str(int(row["RatecodeID"])),
            rate_name=MapRateCodeIdToName(row["RatecodeID"]),
        )
        pickup_id = InsertRecordIntoDatabase(
            models.TaxiMeter,
            database,
            taxi_meter_date=row["tpep_pickup_datetime"].to_pydatetime(),
            taxi_meter_location=row["PULocationID"],
        )
        dropoff_id = InsertRecordIntoDatabase(
            models.TaxiMeter,
            database,
            taxi_meter_date=row["tpep_dropoff_datetime"].to_pydatetime(),
            taxi_meter_location=row["DOLocationID"],
        )
        InsertRecordIntoDatabase(
            models.Vendor,
            database,
            id=str(int(row["VendorID"])),
            vendor_name=MapVendorIdToName(row["VendorID"]),
        )
        fees_id = InsertRecordIntoDatabase(
            models.Fees,
            database,
            mta_tax=row["mta_tax"],
            improvement_surcharge=row["improvement_surcharge"],
            airport_fee=row["Airport_fee"],
            cbd_congestion_fee=row["cbd_congestion_fee"],
        )
        payment_id = InsertRecordIntoDatabase(
            models.Payment,
            database,
            payment_type=row["payment_type"],
            extra=row["extra"],
            tolls_amount=row["tolls_amount"],
            fare_amount=row["fare_amount"],
            total_amount=row["total_amount"],
            fees_id=str(fees_id),
            rate_code_id=str(int(row["RatecodeID"])),
        )
        InsertRecordIntoDatabase(
            models.Trip,
            database,
            distance=row["trip_distance"],
            passenger_count=row["passenger_count"],
            pickup_id=str(pickup_id),
            dropoff_id=str(dropoff_id),
            payment_id=str(payment_id),
            vendor_id=str(int(row["VendorID"])),
        )
        percentage = CalcPercentage(int(str(row_id)) + 1, total_rows)
        if percentage % 10 == 0:
            logging.info(f"Processed {percentage:.1f}% of rows")


def CreateNycTaxiRedisSchema(
    database: AbstractDatabase, index_name: str
) -> None:
    schema: list[Field] = [
        NumericField("$.VendorID", as_name="VendorID"),
        TextField("$.tpep_pickup_datetime", as_name="pickup_time"),
        TextField("$.tpep_dropoff_datetime", as_name="dropoff_time"),
        NumericField("$.passenger_count", as_name="passenger_count"),
        NumericField("$.trip_distance", as_name="trip_distance"),
        NumericField("$.RatecodeID", as_name="RatecodeID"),
        NumericField("$.PULocationID", as_name="PULocationID"),
        NumericField("$.DOLocationID", as_name="DOLocationID"),
        NumericField("$.payment_type", as_name="payment_type"),
        NumericField("$.fare_amount", as_name="fare_amount"),
        NumericField("$.extra", as_name="extra"),
        NumericField("$.mta_tax", as_name="mta_tax"),
        NumericField("$.tip_amount", as_name="tip_amount"),
        NumericField("$.tolls_amount", as_name="tolls_amount"),
        NumericField(
            "$.improvement_surcharge", as_name="improvement_surcharge"
        ),
        NumericField("$.total_amount", as_name="total_amount"),
        NumericField("$.congestion_surcharge", as_name="congestion_surcharge"),
        NumericField("$.Airport_fee", as_name="Airport_fee"),
        NumericField("$.cbd_congestion_fee", as_name="cbd_congestion_fee"),
    ]
    database.GetDatabaseEngine().ft(index_name).create_index(
        schema,
        definition=IndexDefinition(prefix=["trip:"], index_type=IndexType.JSON),
    )


def LoadNycTaxiDataToRedisDatabase(
    database: AbstractDatabase, taxi_data: pd.DataFrame
) -> None:
    trip_index = "idx:trip"
    try:
        database.GetDatabaseEngine().ft(trip_index).info()
    except Exception:
        CreateNycTaxiRedisSchema(database=database, index_name=trip_index)

    redis_handler = RedisCRUDHandler(database.GetDatabaseEngine())
    total_rows = len(taxi_data)
    for row_id, row in taxi_data.iterrows():
        record_dict = row.to_dict()
        record_dict.pop("store_and_fwd_flag")
        record_dict["tpep_pickup_datetime"] = str(
            record_dict["tpep_pickup_datetime"].to_pydatetime()
        )
        record_dict["tpep_dropoff_datetime"] = str(
            record_dict["tpep_dropoff_datetime"].to_pydatetime()
        )

        redis_handler.create(f"trip:{str(row_id)}", record_dict)

        percentage = CalcPercentage(int(str(row_id)) + 1, total_rows)
        if percentage % 10 == 0:
            logging.info(f"Processed {percentage:.1f}% of rows")
