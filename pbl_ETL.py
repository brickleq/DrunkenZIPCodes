# pbl_ETL.py
#%%
# Dependencies
import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy import *
from sqlalchemy.sql import select
from sqlalchemy.orm import Session, sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import database_exists, create_database, drop_database
from config import username, password


#%%
###EXTRACT

# Read .csv data into DataFrames
dfPop2000 = pd.DataFrame # Population by zipcode, 2000
csvFile = 'Resources/population_by_zip_2000.csv'
dfPop2000 = pd.read_csv(csvFile,delimiter=',',encoding='utf-8',low_memory=False,dtype=str)
dfPop2010 = pd.DataFrame # Population by zipcode, 2010
csvFile = 'Resources/population_by_zip_2010.csv'
dfPop2010 = pd.read_csv(csvFile,delimiter=',',encoding='utf-8',low_memory=False,dtype=str)
dfBars = pd.DataFrame # Bars dataset
csvFile = 'Resources/8260_1.csv'
dfBars = pd.read_csv(csvFile,delimiter=',',encoding='utf-8',low_memory=False,dtype=str)


#%%
###TRANSFORM

# Bars DataFrame - dirty data clean-up! (Need to be able to merge on 'zipcode') 
dfBars.dropna(subset=['postalCode'],inplace=True) # Drop rows with no zipcode listed
dfBars['postalCode'] = dfBars['postalCode'].str.zfill(5) # Prepend '0' if zipcode less than 5 digits long
dfBars['postalCode'] = dfBars['postalCode'].str.slice(start=0,stop=5) # Keep only first 5 digits of zipcode
bars_columns = [u'postalCode',u'name'] # the 'u' before column name -should- make sure all data is utf-8 compliant
bars_clean = dfBars[bars_columns].copy()
dfBars=bars_clean.rename(columns={'postalCode':'zipcode'})
dfBars.head()


#%%
# Create new DataFrame to hold count of bars in each zipcode
ZipBars = dfBars.groupby(['zipcode']).count()
ZipBars.sort_values('name',ascending=False,inplace=True)
ZipBars.rename(columns={'name':'bar_count'},inplace=True)
ZipBars.head()


#%%
# Population DataFrames - cleanup
dfPop2000.dropna(subset=['zipcode'],inplace=True) # Drop rows with no zipcode listed
dfPop2010.dropna(subset=['zipcode'],inplace=True) 
dfPop2000['zipcode'] = dfPop2000['zipcode'].str.zfill(5) # Prepend '0' if zip < 5 digits long
dfPop2010['zipcode'] = dfPop2010['zipcode'].str.zfill(5)
dfPop2000['zipcode'] = dfPop2000['zipcode'].str.slice(start=0,stop=5) # Keep only first 5 digits
dfPop2010['zipcode'] = dfPop2010['zipcode'].str.slice(start=0,stop=5)
dfPop2000.head()


#%%
# Convert population data to int to enable aggregation
dfPop2000['population'] = dfPop2010['population'].astype(int)
dfPop2010['population'] = dfPop2010['population'].astype(int)


#%%
# Create DataFrames to hold total population by zipcode
ZipPop2000 = dfPop2000.groupby(['zipcode'])['population'].sum()
ZipPop2010 = dfPop2010.groupby(['zipcode'])['population'].sum()
ZipPop2000.head()


#%%
# Create DataFrames to show population grouped by zipcode and gender
GenderPop2000 = dfPop2000.groupby(['zipcode','gender'])['population'].sum()
GenderPop2010 = dfPop2010.groupby(['zipcode','gender'])['population'].sum()
GenderPop2000.head()


#%%
# Generate dictionaries from DataFrames
bar_names = {'zipcode','name'}
bars_by_zip = {'zipcode','bar_count'}
pop_by_zip2000 = {'zipcode','population'}
pop_by_zip2010 = {'zipcode','population'}
gender2000 = {'zipcode','gender','population'}
gender2010 = {'zipcode','gender','population'}
pop_female2000 = {'zipcode':'population'}
pop_male2000 = {'zipcode':'population'}
pop_female2010 = {'zipcode':'population'}
pop_male2010 = {'zipcode':'population'}

bar_names = dfBars.to_dict()
bars_by_zip = ZipBars.to_dict()
pop_by_zip2000 = ZipPop2000.to_dict()
pop_by_zip2010 = ZipPop2010.to_dict()
gender2000 = GenderPop2000.to_dict()
gender2010 = GenderPop2010.to_dict()
for key, value in gender2000.items():
    if key[1] == 'female': pop_female2000.update({key[0]:value})
    if key[1] == 'male': pop_male2000.update({key[0]:value})
for key, value in gender2010.items():
    if key[1] == 'female': pop_female2010.update({key[0]:value})
    if key[1] == 'male': pop_male2010.update({key[0]:value})


#%%
### LOAD

# Create connection to Bars_db
rds_connection_string = username + ':' + password + '@127.0.0.1/Bars_db' #username and password variables stored locally in config.py
engine = create_engine(f'mysql://{rds_connection_string}',echo=False)
if database_exists(engine.url): #check to see if Bars_db exists...
    drop_database(engine.url) #if it does, delete it...
if not database_exists(engine.url): #check to see if Bars_db exists...
    create_database(engine.url) #if it doesn't, create it
conn = engine.connect()
Base = declarative_base()


#%%
engine.table_names()


#%%
# Define classes to create table schemas

class BarName(Base):
    __tablename__ = 'bar_name'
    id = Column(Integer, primary_key=True,nullable=False)
    zipcode = Column(String(5))
    name = Column(String(255))

class BarCount(Base):
    __tablename__ = 'bar_count'
    id = Column(Integer, primary_key=True,nullable=False)
    zipcode = Column(String(5))
    bar_count = Column(Integer)

class Population2000(Base):
    __tablename__ = 'population_by_zipcode_2000'
    id = Column(Integer, primary_key=True,nullable=False)
    zipcode = Column(String(5))
    population = Column(Integer)

class Population2010(Base):
    __tablename__ = 'population_by_zipcode_2010'
    id = Column(Integer, primary_key=True,nullable=False)
    zipcode = Column(String(5))
    population = Column(Integer)

class MalePopulation2000(Base):
    __tablename__ = 'male_population_by_zipcode_2000'
    id = Column(Integer, primary_key=True,nullable=False)
    zipcode = Column(String(5))
    population = Column(Integer)

class MalePopulation2010(Base):
    __tablename__ = 'male_population_by_zipcode_2010'
    id = Column(Integer, primary_key=True,nullable=False)
    zipcode = Column(String(5))
    population = Column(Integer)
    
class FemalePopulation2000(Base):
    __tablename__ = 'female_population_by_zipcode_2000'
    id = Column(Integer, primary_key=True,nullable=False)
    zipcode = Column(String(5))
    population = Column(Integer)

class FemalePopulation2010(Base):
    __tablename__ = 'female_population_by_zipcode_2010'
    id = Column(Integer, primary_key=True,nullable=False)
    zipcode = Column(String(5))
    population = Column(Integer)
    
Base.metadata.create_all(conn)
tables = []
tables = engine.table_names()
tables


#%%
# Get it to work ONCE

session = Session(bind=engine)
bar = BarName(zipcode='53212',name='The Waterfront Cafe')
session.add(bar)
session.commit()
result = conn.execute('SELECT * FROM bar_name;')
result.fetchall()


#%%
# Create series of loops to make it work a whole bunch of times


#%%
session.close()


#%%



