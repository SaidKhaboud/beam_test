# -*- coding: utf-8 -*-
import os
from typing import Any

from sqlalchemy import Column, DateTime, Integer, String, create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

Base = declarative_base()

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
