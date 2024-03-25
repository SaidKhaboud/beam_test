from datetime import datetime
import logging
import click
from sqlalchemy import Column, DateTime, Integer, String, create_engine, func
from sqlalchemy.orm import sessionmaker, declarative_base

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

Base = declarative_base()

class Raw_Entities(Base):
    __tablename__ = "entities"
    uuid = Column(Integer, primary_key=True, autoincrement=True)
    entity = Column(String, name='entity')
    label = Column(String, name='label')  
    readers = Column(Integer, name='readers')
    timestamp = Column(DateTime, name='timestamp')

    
@click.command()
@click.option('--n', help='Number of top entities', prompt='Enter the number N')
@click.option('--min_ts', help='Minimum timestamp', default=None)
@click.option('--max_ts', help='Maximum timestamp', default=None)
def search_entity(n, min_ts, max_ts):

    logger.info('Connecting to the database...')
    engine = create_engine("postgresql+psycopg2://said:seedtag@localhost:5432/seedtag")
    Session = sessionmaker(bind=engine)
    session = Session()

    logger.info('Performing query...')
    query = session.query(
        Raw_Entities.entity,
        func.sum(Raw_Entities.readers).label('total_readers')
    )

    if min_ts:
        min_ts = datetime.strptime(min_ts, "%Y-%m-%d %H:%M:%S")
        query = query.filter(Raw_Entities.timestamp >= min_ts)
    if max_ts:
        max_ts = datetime.strptime(max_ts, "%Y-%m-%d %H:%M:%S")
        query = query.filter(Raw_Entities.timestamp <= max_ts)

    results = query.group_by(Raw_Entities.entity)\
                   .order_by(func.sum(Raw_Entities.readers).desc())\
                   .limit(n)\
                   .all()

    # Displaying results
    for result in results:
        logger.info(f"Entity: {result.entity}, Total Readers: {result.total_readers}")

if __name__ == "__main__":
    search_entity()
