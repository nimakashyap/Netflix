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

# --new table for listed in, director, country, cast

select show_id, value as director into Netflix_director from netflix_raw cross apply string_split(director,',') 
select show_id, value as country into Netflix_country from netflix_raw cross apply string_split(country,',')
select show_id, value as cast into Netflix_cast from netflix_raw cross apply string_split(cast,',')
select show_id, value as genre into Netflix_genre from netflix_raw cross apply string_split(listed_in,',')

select * from Netflix_director
select * from Netflix_country
select * from Netflix_cast
select * from Netflix_genre

# data maping and fill missing value in country column
select * from netflix_raw
where country is null

select * from netflix_raw
where director = 'Ahishor Solomon'

select * from Netflix_country
where show_id = 's1001'

select * from netflix_raw
where show_id = 's1001'

insert into Netflix_country
select show_id, m.country
from netflix_raw nr 
inner join (
select director,country 
from Netflix_country nc
inner join Netflix_director nd on nc.show_id=nd.show_id
group by director,country) m
on nr.director = m.director
where nr.country is null

# checking missing value in python
df.isna().sum()

# data maping and fill missing value in duration (where duration is null)

with cte as (
select *, ROW_NUMBER()over (partition by title, type order by show_id) as rn 
from netflix_raw)
select show_id, type, title, cast(date_added as date) as date_added, release_year,rating
,case when duration is null then  rating else duration end as duration, description
from cte 
where rn = 1

## create final table added (into netflix in above query) 

with cte as (
select *, ROW_NUMBER()over (partition by title, type order by show_id) as rn 
from netflix_raw)
select show_id, type, title, cast(date_added as date) as date_added, release_year,rating
,case when duration is null then  rating else duration end as duration, description
into Netflix from cte 
where rn = 1

select * from netflix





















