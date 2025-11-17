import typing

import pandas as pd

from .framework import models
from .framework.abstract_database import AbstractDatabase
from .framework.crud_handlers import OrmCRUDHandler, RedisCRUDHandler

ORM_TABLE_TYPE = typing.TypeVar("ORM_TABLE_TYPE", bound=models.BaseOrmType)


def InsertRecordIntoDatabase(
    orm_type: typing.Type[ORM_TABLE_TYPE],
    database: AbstractDatabase,
    **kwargs: str,
) -> typing.Optional[int]:
    orm_handler = OrmCRUDHandler(database.GetDatabaseHandle(), orm_type)

    orm_entry = orm_type(**kwargs)
    try:
        return orm_handler.create(orm_entry)
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


def LoadNycTaxiDataToSqlDatabase(
    database: AbstractDatabase, taxi_data: pd.DataFrame
) -> None:
    total_rows = len(taxi_data)
    for row_id, row in taxi_data.iterrows():
        rate_code_id = InsertRecordIntoDatabase(
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
        vendor_id = InsertRecordIntoDatabase(
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
            rate_code_id=str(rate_code_id),
        )
        InsertRecordIntoDatabase(
            models.Trip,
            database,
            distance=row["trip_distance"],
            passenger_count=row["passenger_count"],
            pickup_id=str(pickup_id),
            dropoff_id=str(dropoff_id),
            payment_id=str(payment_id),
            vendor_id=str(vendor_id),
        )

        if int(str(row_id)) % int(total_rows // 1000) == 0:
            print(f"{int(str(row_id)) / total_rows * 100:.2f}% rows processed.")


def LoadNycTaxiDataToRedisDatabase(
    database: AbstractDatabase, taxi_data: pd.DataFrame
) -> None:
    redis_handler = RedisCRUDHandler(database.GetDatabaseHandle())
    total_rows = len(taxi_data)
    for row_id, row in taxi_data.iterrows():
        record_dict = row.to_dict()
        record_dict["tpep_pickup_datetime"] = str(
            record_dict["tpep_pickup_datetime"].to_pydatetime()
        )
        record_dict["tpep_dropoff_datetime"] = str(
            record_dict["tpep_dropoff_datetime"].to_pydatetime()
        )
        redis_handler.create(str(row_id), record_dict)

        if int(str(row_id)) % int(total_rows // 1000) == 0:
            print(f"{int(str(row_id)) / total_rows * 100:.2f}% rows processed.")
