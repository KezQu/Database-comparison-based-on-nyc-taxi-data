import typing

import pandas as pd

from .framework import models
from .framework.abstract_database import AbstractDatabase
from .framework.crud_handlers import OrmCRUDHandler

ORM_TABLE_TYPE = typing.TypeVar("ORM_TABLE_TYPE", bound=models.BaseOrmType)


def InsertRecordIntoDatabase(
    orm_type: typing.Type[ORM_TABLE_TYPE],
    database: AbstractDatabase,
    **kwargs: str,
) -> None:
    orm_handler = OrmCRUDHandler(database.GetDatabaseHandle(), orm_type)

    orm_entry = orm_type(**kwargs)
    try:
        orm_handler.create(orm_entry)
    except Exception:
        pass


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
        if int(str(row_id)) % int(total_rows // 1000) == 0:
            print(f"{int(str(row_id)) / total_rows * 100:.2f}% rows processed.")

        InsertRecordIntoDatabase(
            models.FareRate,
            database,
            rate_code_id=str(int(row["RatecodeID"])),
            rate_name=MapRateCodeIdToName(row["RatecodeID"]),
        )
        InsertRecordIntoDatabase(
            models.TaxiMeter,
            database,
            taxi_meter_date=row["tpep_pickup_datetime"].to_pydatetime(),
            taxi_meter_location=row["PULocationID"],
        )
        InsertRecordIntoDatabase(
            models.TaxiMeter,
            database,
            taxi_meter_date=row["tpep_dropoff_datetime"].to_pydatetime(),
            taxi_meter_location=row["DOLocationID"],
        )
        InsertRecordIntoDatabase(
            models.Vendor,
            database,
            vendor_id=str(int(row["VendorID"])),
            vendor_name=MapVendorIdToName(row["VendorID"]),
        )
        InsertRecordIntoDatabase(
            models.Fees,
            database,
            mta_tax=row["mta_tax"],
            improvement_surcharge=row["improvement_surcharge"],
            airport_fee=row["Airport_fee"],
            cbd_congestion_fee=row["cbd_congestion_fee"],
        )
        # InsertRecordIntoDatabase(
        #     models.Trip,
        #     database,
        #     distance=row["distance"],
        #     passenger_count=row["passenger_count"],
        #     pickup_id=row["pickup_id"],
        #     dropoff_id=row["dropoff_id"],
        #     payment_id=row["payment_id"],
        #     vendor_id=row["vendor_id"],
        # )
        # InsertRecordIntoDatabase(
        #     models.Payment,
        #     database,
        #     payment_type=row["payment_type"],
        #     extra=row["extra"],
        #     tolls_amount=row["tolls_amount"],
        #     fare_amount=row["fare_amount"],
        #     total_amount=row["total_amount"],
        #     fees_id=row["fees_id"],
        #     rate_code_id=row["rate_code_id"],
        # )


def LoadNycTaxiDataToRedisDatabase(
    database: AbstractDatabase, taxi_data: pd.DataFrame
) -> None:
    raise NotImplementedError("Redis data loader is not implemented yet.")
