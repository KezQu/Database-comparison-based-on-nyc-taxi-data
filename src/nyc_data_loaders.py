import logging
import typing

import pandas as pd
from redis.commands.search.field import Field, NumericField, TagField, TextField
from redis.commands.search.index_definition import IndexDefinition, IndexType

from .framework import models
from .framework.abstract_database import AbstractDatabase
from .framework.crud_handlers import OrmCRUDHandler, RedisCRUDHandler

ORM_TABLE_TYPE = typing.TypeVar("ORM_TABLE_TYPE", bound=models.BaseOrmType)


def InsertBulkRecordsIntoDatabase(
    orm_type: typing.Type[ORM_TABLE_TYPE],
    database: AbstractDatabase,
    filtered_frame: pd.DataFrame,
) -> None:
    orm_handler = OrmCRUDHandler(database.GetDatabaseEngine())
    orm_entries: list[dict[str, typing.Any]] = (
        filtered_frame.drop_duplicates().to_dict(orient="records")
    )
    logging.debug(orm_entries)
    orm_handler.create(
        orm_type,
        *orm_entries,
    )


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


def AddIdForUniqueColumns(
    df: pd.DataFrame, columns: list[str], new_id_column: str
) -> None:
    tuples = df[columns].apply(tuple, axis=1)
    id_map = {val: idx for idx, val in enumerate(tuples.unique())}
    df[new_id_column] = tuples.map(id_map)


def AddIdForConcatenatedColumns(
    df: pd.DataFrame, *columns_with_new_id: tuple[list[str], str]
) -> None:
    tuples_list: list[tuple[pd.Series[typing.Any], str]] = []
    for columns, new_id_column in columns_with_new_id:
        tuples_list.append((df[columns].apply(tuple, axis=1), new_id_column))

    merged_tuples = pd.concat([t[0] for t in tuples_list])
    id_map = {val: idx for idx, val in enumerate(merged_tuples.unique())}
    for tuples, new_id_column in tuples_list:
        df[new_id_column] = tuples.map(id_map)


def LoadNycTaxiDataToSqlDatabase(
    database: AbstractDatabase, taxi_data: pd.DataFrame
) -> None:
    PICKUP_COLUMNS = ["tpep_pickup_datetime", "PULocationID"]
    DROPOFF_COLUMNS = ["tpep_dropoff_datetime", "DOLocationID"]
    FEES_COLUMNS = [
        "mta_tax",
        "improvement_surcharge",
        "airport_fee",
        "cbd_congestion_fee",
    ]
    PAYMENT_COLUMNS = [
        "payment_type",
        "extra",
        "tolls_amount",
        "fare_amount",
        "total_amount",
        "fees_id",
        "rate_code_id",
    ]
    TRIP_COLUMNS = [
        "distance",
        "passenger_count",
        "pickup_id",
        "dropoff_id",
        "payment_id",
        "vendor_id",
    ]
    FARE_RATE_COLUMNS_WITH_ID = ["rate_code_id", "rate_name"]
    VENDOR_COLUMNS_WITH_ID = ["vendor_id", "vendor_name"]
    PICKUP_COLUMNS_WITH_ID = PICKUP_COLUMNS + ["pickup_id"]
    DROPOFF_COLUMNS_WITH_ID = DROPOFF_COLUMNS + ["dropoff_id"]
    FEES_COLUMNS_WITH_ID = FEES_COLUMNS + ["fees_id"]
    PAYMENT_COLUMNS_WITH_ID = PAYMENT_COLUMNS + ["payment_id"]
    TRIP_COLUMNS_WITH_ID = TRIP_COLUMNS + ["trip_id"]
    taxi_data["rate_name"] = taxi_data["rate_code_id"].apply(
        MapRateCodeIdToName
    )
    taxi_data["vendor_name"] = taxi_data["vendor_id"].apply(MapVendorIdToName)
    AddIdForConcatenatedColumns(
        taxi_data,
        (PICKUP_COLUMNS, "pickup_id"),
        (DROPOFF_COLUMNS, "dropoff_id"),
    )
    AddIdForUniqueColumns(taxi_data, FEES_COLUMNS, "fees_id")
    AddIdForUniqueColumns(taxi_data, PAYMENT_COLUMNS, "payment_id")
    AddIdForUniqueColumns(taxi_data, TRIP_COLUMNS, "trip_id")

    table_types_with_dataframes: list[
        tuple[typing.Type[models.BaseOrmType], pd.DataFrame]
    ] = [
        (
            models.FareRate,
            taxi_data[FARE_RATE_COLUMNS_WITH_ID].rename(
                columns={"rate_code_id": "id"}
            ),
        ),
        (
            models.TaxiMeter,
            pd.concat(
                [
                    taxi_data[PICKUP_COLUMNS_WITH_ID].rename(
                        columns={
                            "pickup_id": "id",
                            "tpep_pickup_datetime": "taxi_meter_date",
                            "PULocationID": "taxi_meter_location",
                        }
                    ),
                    taxi_data[DROPOFF_COLUMNS_WITH_ID].rename(
                        columns={
                            "dropoff_id": "id",
                            "tpep_dropoff_datetime": "taxi_meter_date",
                            "DOLocationID": "taxi_meter_location",
                        }
                    ),
                ]
            ),
        ),
        (
            models.Vendor,
            taxi_data[VENDOR_COLUMNS_WITH_ID].rename(
                columns={"vendor_id": "id"}
            ),
        ),
        (
            models.Fees,
            taxi_data[FEES_COLUMNS_WITH_ID].rename(columns={"fees_id": "id"}),
        ),
        (
            models.Payment,
            taxi_data[PAYMENT_COLUMNS_WITH_ID].rename(
                columns={"payment_id": "id"}
            ),
        ),
        (
            models.Trip,
            taxi_data[TRIP_COLUMNS_WITH_ID].rename(columns={"trip_id": "id"}),
        ),
    ]
    for table_type, dataframe in table_types_with_dataframes:
        InsertBulkRecordsIntoDatabase(
            table_type,
            database,
            dataframe,
        )


def CreateNycTaxiRedisSchema(
    database: AbstractDatabase, index_name: str
) -> None:
    schema: list[Field] = [
        TagField("$.vendor_name", as_name="vendor_name"),
        TextField("$.tpep_pickup_datetime", as_name="pickup_time"),
        TextField("$.tpep_dropoff_datetime", as_name="dropoff_time"),
        NumericField("$.passenger_count", as_name="passenger_count"),
        NumericField("$.distance", as_name="distance"),
        NumericField("$.rate_code_id", as_name="rate_code_id"),
        TagField("$.fare_rate", as_name="fare_rate"),
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
        NumericField("$.airport_fee", as_name="airport_fee"),
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
    for row_id, row in taxi_data.iterrows():
        record_dict = row.to_dict()
        record_dict["fare_rate"] = MapRateCodeIdToName(
            record_dict["rate_code_id"]
        )
        record_dict["vendor_name"] = MapVendorIdToName(record_dict["vendor_id"])
        record_dict["tpep_pickup_datetime"] = str(
            record_dict["tpep_pickup_datetime"].to_pydatetime()
        )
        record_dict["tpep_dropoff_datetime"] = str(
            record_dict["tpep_dropoff_datetime"].to_pydatetime()
        )
        record_dict.pop("store_and_fwd_flag")
        record_dict.pop("vendor_id")

        redis_handler.create(f"trip:{str(row_id)}", record_dict)
