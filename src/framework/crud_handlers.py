import contextlib
import json
import logging
import typing
from abc import ABC

from redis.commands.json.path import Path
from redis.commands.search.document import Document
from redis.commands.search.query import Query
from sqlalchemy import Delete, Engine, Result, Update, insert
from sqlalchemy.orm import Session
from sqlalchemy.sql.selectable import TypedReturnsRows

from redis import Redis

from .models import BaseOrmType


class AbstractCRUDHandler(ABC):
    pass


ORM_TABLE_TYPE = typing.TypeVar("ORM_TABLE_TYPE", bound=BaseOrmType)


class RedisCRUDHandler(AbstractCRUDHandler):
    def __init__(self, db_engine: Redis):
        self.db_engine = db_engine

    def create(self, entry_id: str, entry: dict[str, typing.Any]) -> None:
        self.db_engine.json().set(entry_id, Path.root_path(), entry)

    def read(
        self, indexed_query: tuple[str, Query]
    ) -> dict[str, dict[str, typing.Any]]:
        index_name, query = indexed_query
        logging.debug(f"Executing Redis query: {query.query_string()}")
        max_results = int(self.db_engine.ft(index_name).search(query).total)  # type: ignore

        found_entries: list[Document] = (  # type: ignore
            self.db_engine.ft(index_name)
            .search(query.paging(0, max_results))
            .docs  # type: ignore
        )
        logging.info(f"Found {max_results} entries matching the query.")

        dict_entries: dict[str, dict[str, typing.Any]] = {}
        for document in found_entries:  # type: ignore
            doc_json: dict[str, typing.Any] = json.loads(document.json)  # type: ignore
            dict_entries[document.id] = doc_json  # type: ignore
        logging.debug(f"Redis read query result: {dict_entries}")
        return dict_entries

    def update(
        self,
        indexed_query: tuple[str, Query],
        values: dict[typing.Any, typing.Any],
    ) -> typing.Optional[int]:
        query_results = self.read(indexed_query)
        for entry_id, entry in query_results.items():
            for field, value in values.items():
                entry[field] = value
            logging.debug(f"Updating entry ID {entry_id} with values {entry}")
            self.db_engine.json().set(entry_id, Path.root_path(), entry)
        logging.info(f"Updated {len(query_results)} entries.")
        return len(query_results)

    def delete(self, indexed_query: tuple[str, Query]) -> None:
        query_results = self.read(indexed_query)
        if not query_results:
            logging.warning("No matching records found to delete.")
        for entry_id in query_results.keys():
            self.db_engine.json().delete(entry_id)
        logging.info(f"Deleted {len(query_results)} entries.")


class OrmCRUDHandler(AbstractCRUDHandler):
    def __init__(self, db_engine: Engine):
        self.db_engine = db_engine

    @contextlib.contextmanager
    def _establish_session(self):
        with Session(self.db_engine) as session:
            with session.begin():
                yield session

    def create(
        self,
        orm_type: typing.Type[ORM_TABLE_TYPE],
        **orm_fields: str,
    ) -> int:
        with self._establish_session() as session:
            result = session.execute(insert(orm_type).values(**orm_fields))
        logging.debug(
            f"Created ORM {orm_type.__name__} with ID: {result.inserted_primary_key[0]} : {orm_fields}"  # type: ignore
        )
        return result.inserted_primary_key[0]  # type: ignore

    def bulk_create(self, orm_entries: list[BaseOrmType]) -> None:
        with self._establish_session() as session:
            session.bulk_save_objects(orm_entries)

    def read(
        self, query: TypedReturnsRows[typing.Tuple[ORM_TABLE_TYPE]]
    ) -> list[dict[str, typing.Any] | str]:
        logging.debug(f"Executing ORM read query: {query}")
        with self._establish_session() as session:
            found_entries = session.scalars(query).all()
            converted_entries = [
                entry.to_dict() if hasattr(entry, "to_dict") else str(entry)
                for entry in found_entries
            ]
            logging.info(
                f"Found {len(found_entries)} entries matching the query."
            )
            logging.debug(f"ORM read query result: {converted_entries}")
        return converted_entries

    def update(
        self,
        query: Update,
        values: dict[typing.Any, typing.Any],
    ) -> Result[typing.Any]:
        logging.debug(
            f"Executing ORM update query: {query} with values {values}."
        )
        with self._establish_session() as session:
            update_result = session.execute(query.values(**values))
        logging.info(f"Updated {update_result.rowcount} entries.")
        return update_result

    def delete(self, query: Delete) -> Result[typing.Any]:
        logging.debug(f"Executing ORM delete query: {query}.")
        with self._establish_session() as session:
            delete_result = session.execute(query)
            if not delete_result.rowcount:
                logging.warning("No matching records found to delete.")
            logging.info(f"Deleted {delete_result.rowcount} entries.")
        return delete_result
