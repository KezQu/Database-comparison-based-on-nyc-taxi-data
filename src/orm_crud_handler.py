import contextlib
import typing

from sqlalchemy import ColumnElement, Engine
from sqlalchemy.orm import Session

from models import BaseOrmType

ORM_TABLE_TYPE = typing.TypeVar("ORM_TABLE_TYPE", bound=BaseOrmType)


class OrmCRUDHandler(typing.Generic[ORM_TABLE_TYPE]):
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

    def insert(self, orm_entry: ORM_TABLE_TYPE) -> None:
        with self._establish_session() as session:
            session.add(orm_entry)

    def get_all(self) -> list[dict[str, typing.Any]]:
        with self._establish_session() as session:
            found_entries = session.query(self.orm_type).all()
            converted_entries = [entry.to_dict() for entry in found_entries]
        return converted_entries

    def get(self, query: ColumnElement[bool]) -> list[dict[str, typing.Any]]:
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
