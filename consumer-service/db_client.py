from sqlalchemy import create_engine, Column, String, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

Base = declarative_base()

class Raw_Entities(Base):
    __tablename__ = "drugs"
    entity = Column(String, primary_key=True)
    label = Column(String)
    readers = Column(Integer)
    timestamp = Column(DateTime)

class DBClient:
    def __init__(self, connection_string):
        self.engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.engine)
        Base.metadata.create_all(self.engine)

    def init_table(self):
        Base.metadata.tables['drugs'].create(bind=self.engine, checkfirst=True)

    def write(self, entity, label, readers, timestamp):
        session = self.Session()
        new_entry = Raw_Entities(entity=entity, label=label, readers=readers, timestamp=timestamp)
        session.add(new_entry)
        session.commit()
        session.close()
