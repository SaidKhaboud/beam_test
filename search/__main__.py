import datetime
from datetime import datetime

import click
from sqlalchemy import Column, DateTime, Integer, String, create_engine
from sqlalchemy.orm import sessionmaker
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Raw_Entities:
    __tablename__ = "drugs"
    uuid = Column(Integer, primary_key=True, autoincrement=True)
    entity = Column(String)
    label = Column(String)
    readers = Column(Integer)
    timestamp = Column(DateTime)

    
@click.command(name='search_entity')
@click.option('--entity', help='Entity to search for')
@click.option('--min_timestamp', help='Minimum timestamp')
@click.option('--max_timestamp', help='Maximum timestamp')
def search_entity(entity, min_timestamp, max_timestamp):

    logger.info('Connecting to the database...')
    engine = create_engine("postgresql+psycopg2://said:seedtag@postgres:5432/seedtag")
    Session = sessionmaker(bind=engine)
    session = Session()

    logger.info('Converting timestamps...')
    min_timestamp = datetime.datetime.strptime(min_timestamp, "%Y-%m-%d %H:%M:%S")
    max_timestamp = datetime.datetime.strptime(max_timestamp, "%Y-%m-%d %H:%M:%S")

    logger.info('Performing query...')
    results = session.query(Raw_Entities).filter(
        Raw_Entities.entity == entity,
        Raw_Entities.timestamp >= min_timestamp,
        Raw_Entities.timestamp <= max_timestamp
    ).all()

    logger.info('Printing results...')
    for result in results:
        print(result)

    logger.info('Search completed.')