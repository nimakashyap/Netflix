# Netflix
Netfilx data

# importd file in Python
import pandas as pd
df = pd.read_csv(r"C:\Users\NimaKashyap\.kaggle\netflix_titles.csv")

# make connection from SQL
import sqlalchemy as sal   # import library
connection_string = "mssql+pyodbc://DESKTOP-CVURE3B/Practice1?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"   # connection string got from SQL
engine = sal.create_engine(connection_string)   # creates a connection engine to interact with a SQL database
conn = engine.connect()   # connect from SQL
query = "select * from netflix_titles"  # variable to see data
df1 = pd.read_sql(query, engine)  # see data

# create table in SQL
create table [dbo].[netflix_raw](
	[show_id] [varchar](10) primary key,
	[type] [nvarchar](10) NULL,
	[title] [nvarchar](200) NULL,
	[director] [varchar](250) NULL,
	[cast] [varchar](1050) NULL,
	[country] [varchar](150) NULL,
	[date_added] [varchar] (20) NULL,
	[release_year] [int] NULL,
	[rating] [varchar](10) NULL,
	[duration] [varchar](150) NULL,
	[listed_in] [varchar](100) NULL,
	[description] [varchar](500) NULL)

 # append the data in SQL database
df.to_sql('netflix_raw', con=conn, index=False, if_exists='append')


