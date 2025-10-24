import sys
import oracledb
from sqlalchemy import create_engine, Column, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
class articleParse(Base):
    __tablename__ = 'bwa_articles'
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(Text)
    date = Column(String(30))   
    source = Column(Text)
    city = Column(String(50))
    body = Column(Text)

class llmParse(Base):
    __tablename__ = 'bwa_final'
    id = Column(Integer, primary_key=True, autoincrement=True)
    start_date = Column(String(50), nullable=True)
    end_date = Column(String(50), nullable=True)
    affected_population = Column(Integer, nullable=True)
    city = Column(String(100), nullable=True)
    state = Column(String(50), nullable=True)
    county = Column(String(100), nullable=True)
    utility_name = Column(String(255), nullable=True)
    cause = Column(String(255), nullable=True)
    title = Column(String(255), nullable=True)
    source = Column(Text, nullable=True)