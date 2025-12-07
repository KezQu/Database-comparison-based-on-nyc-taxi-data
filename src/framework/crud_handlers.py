import contextlib
import typing
from abc import ABC

from redis.commands.json.path import Path
from redis.commands.search.query import Query
from sqlalchemy import ColumnElement, Engine
from sqlalchemy.orm import Session

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

    def read_all(self) -> list[dict[str, typing.Any]]:
        all_data: list[dict[str, typing.Any]] = []
        for key in self.db_engine.scan_iter():  # type: ignore
            value = self.db_engine.json().get(key)  # type: ignore
            all_data.append({key: value})
        return all_data

    def read(self, query: Query) -> list[dict[str, typing.Any]]:
        result = self.db_engine.ft().search(query).docs  # type: ignore
        return result  # type: ignore

    def update(
        self,
        query: Query,
        values: dict[typing.Any, typing.Any],
    ) -> typing.Optional[int]:
        query_results = self.read(query)
        for entry in query_results:
            for field, value in values.items():
                entry[field] = value
            self.db_engine.json().set(entry["id"], Path.root_path(), entry)  # type: ignore
        return len(query_results)

    def delete(self, query: Query) -> None:
        query_results = self.read(query)
        if not query_results:
            raise ValueError("No matching records found to delete.")
        for entry in query_results:
            self.db_engine.json().delete(entry["id"])  # type: ignore


class OrmCRUDHandler(AbstractCRUDHandler, typing.Generic[ORM_TABLE_TYPE]):
    def __init__(
        self, db_engine: Engine, orm_type: typing.Type[ORM_TABLE_TYPE]
    ):
        self.db_engine = db_engine
        self.orm_type = orm_type

    @contextlib.contextmanager
    def _establish_session(self):
        with Session(self.db_engine) as session:
            with session.begin():
                yield session

    def create(self, orm_entry: ORM_TABLE_TYPE) -> int:
        with self._establish_session() as session:
            session.add(orm_entry)
            return orm_entry.id  # type: ignore

    def bulk_create(self, orm_entries: list[ORM_TABLE_TYPE]) -> None:
        with self._establish_session() as session:
            session.bulk_save_objects(orm_entries)

    def read_all(self) -> list[dict[str, typing.Any]]:
        with self._establish_session() as session:
            found_entries = session.query(self.orm_type).all()
            converted_entries = [entry.to_dict() for entry in found_entries]
        return converted_entries

    def read(self, query: ColumnElement[bool]) -> list[dict[str, typing.Any]]:
        with self._establish_session() as session:
            found_entries = session.query(self.orm_type).filter(query).all()
            converted_entries = [entry.to_dict() for entry in found_entries]
        return converted_entries

    def update(
        self,
        query: ColumnElement[bool],
        values: dict[typing.Any, typing.Any],
    ) -> typing.Optional[int]:
        entries_updated = None
        with self._establish_session() as session:
            entries_updated = (
                session.query(self.orm_type).filter(query).update(values)
            )
        return entries_updated

    def delete(self, query: ColumnElement[bool]) -> None:
        with self._establish_session() as session:
            found_entries_query = session.query(self.orm_type).filter(query)
            if not len(found_entries_query.all()):
                raise ValueError("No matching records found to delete.")
            found_entries_query.delete()
