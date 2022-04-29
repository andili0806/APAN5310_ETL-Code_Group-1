import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import sqlalchemy
import psycopg2
import warnings

warnings.filterwarnings('ignore')

# Define the connection URL:
# conn_url = 'postgresql://postgres:123@localhost/test5'
hostname = 'pgsql1.c0ttp4tkwo66.us-east-1.rds.amazonaws.com'
username = 'pgsql1'
password = 'columbia1'
database = 'pgsql1'

conn_url = 'postgresql://' + username + ':'+password + '@' + hostname +'/test_python2'
# Create an engine that connects to PostgreSQL:
engine = create_engine(conn_url)

# Establish a connection:
connection = engine.connect()

# Pass the SQL statement
stmt = """
CREATE TABLE project_info (
  project_number varchar(50) primary key,
  legacy_project_number varchar(50),
  sector varchar(50),
  program_type varchar(100),
  project_cost numeric(20,2),
  incentive numeric(20,2),
  total_nameplate_kW_DC numeric(10,2),
  expected_KWh_annual_production numeric(10,2),
  georeference varchar(50)
);


CREATE TABLE status(
       status_id         varchar(30)      NOT NULL,
       project_status    VARCHAR(30),
       PRIMARY KEY (status_id)
    );

CREATE TABLE project_status(
       project_number   varchar(50)      NOT NULL,
       status_id        VARCHAR(30)      NOT NULL,
       FOREIGN KEY (project_number) REFERENCES project_info (project_number),
       FOREIGN KEY (status_id) REFERENCES status(status_id)
    );

CREATE TABLE dates (
        date_id  varchar(30),
        date_application_received  VARCHAR(100),
        date_completed  VARCHAR(100),
        PRIMARY KEY (date_id)
);

CREATE TABLE project_dates (
        project_number  varchar(50),
        date_id  varchar(30),
        PRIMARY KEY (project_number, date_id),
        FOREIGN KEY (project_number) REFERENCES project_info (project_number),
        FOREIGN KEY (date_id)  REFERENCES dates(date_id)
);


CREATE TABLE contractor (
        contractor_id        varchar(100) primary key,
        contractor                varchar(100)
);

CREATE TABLE project_contractor(
        project_number        varchar(50) NOT NULL,
        contractor_id        varchar(100) NOT NULL,
        FOREIGN KEY (project_number) REFERENCES project_info (project_number),
        FOREIGN KEY (contractor_id) REFERENCES contractor(contractor_id)
);


CREATE TABLE solicitation (
  solicitation_number varchar(30) primary key,
  solicitation_name varchar(100)
);

CREATE TABLE project_solicitation (
  project_number varchar(50),
  solicitation_number varchar(30),
  foreign key (project_number) references project_info (project_number),
  foreign key (solicitation_number) references solicitation(solicitation_number)
);


create table location (
      location_id  varchar(30),
      city        varchar(50),
      county      varchar(50),
      state       varchar(50),
      zip_code        char(5),
      primary key (location_id)
      );

create table project_location (
    project_number varchar(50),
    location_id varchar(30),
    primary key (project_number),
    foreign key (project_number) references project_info (project_number),
    foreign key (location_id) references location (location_id)
 );

create table inverter (
  inverter_id   varchar(30),
  primary_inverter_manufacturer varchar(80),
  primary_inverter_model_number varchar(50),
  total_inverter_quantity numeric(10,1),
  primary key (inverter_id)
  );

create table project_inverter (
  project_number varchar(50),
  inverter_id varchar(30),
  primary key (project_number),
  foreign key (project_number) references project_info (project_number),
  foreign key (inverter_id) references inverter(inverter_id)
  );

CREATE TABLE utility (
        utility_id  varchar(30),
        electric_utility  varchar(100),
        PRIMARY KEY (utility_id)
);

CREATE TABLE project_utility (
    project_number  varchar(50),
    utility_id  varchar(30),
    PRIMARY KEY (project_number, utility_id),
    FOREIGN KEY (project_number) REFERENCES project_info (project_number),
    FOREIGN KEY (utility_id) REFERENCES utility(utility_id)
);

CREATE TABLE pv (
        pv_id varchar(30) primary key,
        primary_pv_module_manufacturer  varchar(100),
        pv_module_model_number   varchar(50),
        total_pv_module_quantity   numeric(10,1)
);

CREATE TABLE project_pv(
    project_number   varchar(50) NOT NULL,
    pv_id  varchar(30) NOT NULL,
    FOREIGN KEY (project_number) REFERENCES project_info(project_number),
    FOREIGN KEY (pv_id) REFERENCES pv(pv_id)
);

CREATE TABLE plan (
        solar_plan_id        varchar(30) primary key,
        remote_net_metering        varchar(30),
        affordable_solar        varchar(30),
        community_distributed_generation       varchar(30),
        green_jobs_green_new_york_participant        varchar(30)
);

CREATE TABLE project_plan (
        project_number         varchar(50) primary key,
        solar_plan_id        varchar(30),
        FOREIGN KEY (project_number) REFERENCES project_info (project_number),
        FOREIGN KEY (solar_plan_id) REFERENCES plan(solar_plan_id)
);


"""
# Execute the statement to create tables
results = connection.execute(stmt)

# Splitting data
from google.colab import drive
drive.mount('/content/drive')

import random
data = pd.read_csv('/content/drive/MyDrive/5310 sql/Solar_Electric_Programs_Reported_by_NYSERDA__Beginning_2000.csv')
# data = pd.read_csv('/Users/yibo_ge/Desktop/5310/Group Project/Data/Solar_Electric_Programs_Reported_by_NYSERDA__Beginning_2000.csv')
data.columns = data.columns.str.replace(' ', '_')
data = data.rename(columns={"$Incentive": "Incentive"})
# data = data.fillna('')
data= data.sample(n = 100)

# Table 1
project_info = data[['Project_Number','Legacy_Project_Number','Sector','Program_Type','Project_Cost','Incentive','Total_Nameplate_kW_DC','Expected_KWh_Annual_Production','Georeference']]
project_info.drop_duplicates(subset=['Project_Number'],keep = 'first')

for rows in project_info.itertuples():
    connection.execute('''
                INSERT INTO project_info
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);
                ''',
                rows.Project_Number,
                rows.Legacy_Project_Number,
                rows.Sector,
                rows.Program_Type,
                rows.Project_Cost,
                rows.Incentive,
                rows.Total_Nameplate_kW_DC,
                rows.Expected_KWh_Annual_Production,
                rows.Georeference
                )
import randoma
import randomb
import random

# Table 3&13
data3 = data[['Project_Number','Project_Status']]
project_status=data[['Project_Status']]
project_status.drop_duplicates(inplace=True,keep='first')
project_status['Status_id']= range(1, 1 + len(project_status))
df3 = pd.merge(data3, project_status, how="left", on=['Project_Status'])
program_status=df3[['Project_Number','Status_id']]

for rows in project_status.itertuples():
    connection.execute('''
                INSERT INTO status
                VALUES (%s,%s);
                ''',
                rows.Status_id,
                rows.Project_Status
                )
                
for rows in program_status.itertuples():
    connection.execute('''
                INSERT INTO project_status
                VALUES (%s,%s);
                ''',
                rows.Project_Number,
                rows.Status_id   
                )

# Table 4&14
data4 = data[['Project_Number','Date_Application_Received', 'Date_Completed']]
project_process_dates_df2=data[['Date_Application_Received', 'Date_Completed']]
project_process_dates_df2.drop_duplicates(inplace=True,keep='first')
project_process_dates_df2['date_id']= range(1, 1 + len(project_process_dates_df2))

df4 = pd.merge(data4, project_process_dates_df2, how="left", on=['Date_Application_Received', 'Date_Completed'])
project_dates_df=df4[['Project_Number','date_id']]

for rows in project_process_dates_df2.itertuples():
    connection.execute('''
                INSERT INTO dates
                VALUES (%s,%s,%s);
                ''',
                rows.date_id,
                rows.Date_Application_Received,
                rows.Date_Completed
                )
                
for rows in project_dates_df.itertuples():
    connection.execute('''
                INSERT INTO project_dates
                VALUES (%s,%s);
                ''',
                rows.Project_Number,
                rows.date_id
                )
                
# Table 5&15
data5 = data[['Project_Number','Contractor']]
contractor_df1=data[['Contractor']]
contractor_df1.drop_duplicates(inplace=True,keep='first')
contractor_df1['contractor_id']= range(1, 1 + len(contractor_df1))
df5 = pd.merge(data5, contractor_df1, how="left", on=['Contractor'])
project_contractor=df5[['Project_Number','contractor_id']]

for rows in contractor_df1.itertuples():
    connection.execute('''
                INSERT INTO contractor
                VALUES (%s,%s);
                ''',
                rows.contractor_id,
                rows.Contractor
                )
                
for rows in project_contractor.itertuples():
    connection.execute('''
                INSERT INTO project_contractor
                VALUES (%s,%s);
                ''',
                rows.Project_Number,
                rows.contractor_id
                )
                
# Table 6&16
data6 = data[['Project_Number','Solicitation']]
solicitation=data[['Solicitation']]
solicitation.drop_duplicates(inplace=True,keep='first')
solicitation['solicitation_number']= range(1, 1 + len(solicitation))
df6 = pd.merge(data6, solicitation, how="left", on=['Solicitation'])
project_solicitation=df6[['Project_Number','solicitation_number']]

for rows in solicitation.itertuples():
    connection.execute('''
                INSERT INTO solicitation
                VALUES (%s,%s);
                ''',
                rows.solicitation_number,
                rows.Solicitation
                )
    
for rows in project_solicitation.itertuples():
    connection.execute('''
                INSERT INTO project_solicitation
                VALUES (%s,%s);
                ''',
                rows.Project_Number,
                rows.solicitation_number   
                )
               
# Table 7&17
data7 = data[['Project_Number','City','County','State','Zip_Code']]
location_df=data[['City','County','State','Zip_Code']]
location_df.drop_duplicates(inplace=True,keep='first')
location_df['location_id']= range(1, 1 + len(location_df))
df7 = pd.merge(data7, location_df, how="left", on=['City','County','State','Zip_Code'])
project_location_df=df7[['Project_Number','location_id']]

for rows in location_df.itertuples():
    connection.execute('''
                INSERT INTO location
                VALUES (%s,%s,%s,%s,%s);
                ''',
                rows.location_id,
                rows.City,
                rows.County,
                rows.State,
                rows.Zip_Code
                )

for rows in project_location_df.itertuples():
    connection.execute('''
                INSERT INTO project_location
                VALUES (%s,%s);
                ''',
                rows.Project_Number,
                rows.location_id   
                )
                
# Table 8&18
# inverter dataframe
data8 = data[['Project_Number','Primary_Inverter_Manufacturer','Primary_Inverter_Model_Number','Total_Inverter_Quantity']]
inverter_df=data[['Primary_Inverter_Manufacturer','Primary_Inverter_Model_Number','Total_Inverter_Quantity']]
inverter_df.drop_duplicates(inplace=True,keep='first')
inverter_df['inverter_id'] = range(1, 1 + len(inverter_df))

# macth project_number
df8 = pd.merge(data8, inverter_df, how="left", on=['Primary_Inverter_Manufacturer','Primary_Inverter_Model_Number','Total_Inverter_Quantity'])
project_inverter_df=df8[['Project_Number','inverter_id']]

for rows in inverter_df.itertuples():
    connection.execute('''
                INSERT INTO inverter
                VALUES (%s,%s,%s,%s);
                ''',
                rows.inverter_id,
                rows.Primary_Inverter_Manufacturer,
                rows.Primary_Inverter_Model_Number,
                rows.Total_Inverter_Quantity
                )

for rows in project_inverter_df.itertuples():
    connection.execute('''
                INSERT INTO project_inverter
                VALUES (%s,%s);
                ''',
                rows.Project_Number,
                rows.inverter_id
                )
                
# Table 9&19
# inverter dataframe
data9 = data[['Project_Number','Electric_Utility']]
electric_utility_df=data[['Electric_Utility']]
electric_utility_df.drop_duplicates(inplace=True,keep='first')
electric_utility_df['utility_id'] = range(1, 1 + len(electric_utility_df))

# macth project_number
df9 = pd.merge(data9, electric_utility_df, how="left", on=['Electric_Utility'])
project_utility_df=df9[['Project_Number','utility_id']]

for rows in electric_utility_df.itertuples():
    connection.execute('''
                INSERT INTO utility
                VALUES (%s,%s);
                ''',
                rows.utility_id,
                rows.Electric_Utility              
                )
    
for rows in project_utility_df.itertuples():
    connection.execute('''
                INSERT INTO project_utility
                VALUES (%s,%s);
                ''',
                rows.Project_Number,
                rows.utility_id
                )
                
# Table 10&20
# inverter dataframe
data10 = data[['Project_Number','Primary_PV_Module_Manufacturer', 'PV_Module_Model_Number','Total_PV_Module_Quantity']]
primary_pv_df=data[['Primary_PV_Module_Manufacturer', 'PV_Module_Model_Number','Total_PV_Module_Quantity']]
primary_pv_df.drop_duplicates(inplace=True,keep='first')
primary_pv_df['pv_id'] = range(1, 1 + len(primary_pv_df))

# macth project_number
df10 = pd.merge(data10, primary_pv_df, how="left", on=['Primary_PV_Module_Manufacturer', 'PV_Module_Model_Number','Total_PV_Module_Quantity'])
project_primary_pv_df=df10[['Project_Number','pv_id']]

for rows in primary_pv_df.itertuples():
    connection.execute('''
                INSERT INTO pv
                VALUES (%s,%s,%s,%s);
                ''',
                rows.pv_id,
                rows.Primary_PV_Module_Manufacturer,
                rows.PV_Module_Model_Number,
                rows.Total_PV_Module_Quantity
                )
                
for rows in project_primary_pv_df.itertuples():
    connection.execute('''
                INSERT INTO project_pv
                VALUES (%s,%s);
                ''',
                rows.Project_Number,
                rows.pv_id
                )
                
# Table 11&21
# inverter dataframe
data11 = data[['Project_Number','Remote_Net_Metering','Affordable_Solar','Community_Distributed_Generation','Green_Jobs_Green_New_York_Participant']]
solar_type=data[['Remote_Net_Metering','Affordable_Solar','Community_Distributed_Generation','Green_Jobs_Green_New_York_Participant']]
solar_type.drop_duplicates(inplace=True,keep='first')
solar_type['solar_plan_id'] = range(1, 1 + len(solar_type))

# macth project_number
df11 = pd.merge(data11, solar_type, how="left", on=['Remote_Net_Metering','Affordable_Solar','Community_Distributed_Generation','Green_Jobs_Green_New_York_Participant'])
project_solar_type=df11[['Project_Number','solar_plan_id']]

# insert into TABLE project_solar_type
for rows in solar_type.itertuples():
    connection.execute('''
                INSERT INTO plan
                VALUES (%s,%s,%s,%s,%s);
                ''',
                rows.solar_plan_id,
                rows.Remote_Net_Metering,
                rows.Affordable_Solar,
                rows.Community_Distributed_Generation,
                rows.Green_Jobs_Green_New_York_Participant
                )
                
# insert into TABLE project_solar_type
for rows in project_solar_type.itertuples():
    connection.execute('''
                INSERT INTO project_plan
                VALUES (%s,%s);
                ''',
                rows.Project_Number,
                rows.solar_plan_id
                )

