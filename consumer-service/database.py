# -*- coding: utf-8 -*-
import os
from typing import Any

from sqlalchemy import Column, DateTime, Integer, String, create_engine
from sqlalchemy.ext.declarative import as_declarative, declarative_base, declared_attr
from sqlalchemy.orm import sessionmaker

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

@as_declarative()
class Base:
    id: Any
    __name__: str
    # Generate __tablename__ automatically

    @declared_attr
    def __tablename__(cls) -> str:
        return cls.__name__.lower()

    def duplicate(self, **kwargs):
        """
        Duplicate an object, and optionally change some of its attributes
        """
        new_obj = self.__class__()
        for column in self.__table__.columns:
            if column.name not in kwargs:
                setattr(new_obj, column.name, getattr(self, column.name))
            else:
                setattr(new_obj, column.name, kwargs[column.name])
        return new_obj


class Raw_Entities(Base):
    __tablename__ = "entities"
    uuid = Column(Integer, primary_key=True, autoincrement=True)
    entity = Column(String, name='entity')
    label = Column(String, name='label')  
    readers = Column(Integer, name='readers')
    timestamp = Column(DateTime, name='timestamp')


db_connection_string = "postgresql+psycopg2://said:seedtag@postgres:5432/seedtag"
engine = create_engine(db_connection_string)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base.metadata.create_all(bind=engine)

# databsse setup
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
    return db
