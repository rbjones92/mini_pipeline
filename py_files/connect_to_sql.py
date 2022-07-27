# Robert Jones
# 7.26.22
# Pipeline Mini-Project
# ...Take ticket data (.csv), enter in MySQL, query table for most popular event. 

import sqlalchemy
from sqlalchemy import create_engine
import datetime
import pandas as pd
from tabulate import tabulate
import re

# Path for data
csv_path = ('C:/Users/Robert.Jones/Desktop/Springboard/mini_pipeline/ticket_data/third_party_sales.csv')
filename = csv_path.rsplit('/',1)[1]
# Credentials for sql login
cred_file = open('C:/Users/Robert.Jones/Desktop/Springboard/mini_pipeline/mysql_files/creds.txt','r')
creds = cred_file.readlines()[0]

class MiniPipeline:

    ### Get MYSQL Connection
    def get_db_connection():
        '''
        Create connection to MySQL database
        '''

        try:
            # Create engine using login credentials 
            engine = create_engine(f'{creds}')
            connection = engine.connect()

        # Print error if this fails
        except Exception as error:
            print("Error while connecting to database for job tracker", error)

        return connection


    def load_third_party(path):
        '''
        Create table in MySQL, load data from .csv file into table. 
        '''
        
        # Call function to get connection to MySQL
        connection = MiniPipeline.get_db_connection()

        ###### Create Table ######
        meta = sqlalchemy.MetaData()

        ticket_table = sqlalchemy.Table(
            'ticket_table',meta,
            sqlalchemy.Column('ticket_id',sqlalchemy.Integer()),
            sqlalchemy.Column('trans_date',sqlalchemy.DATE()),
            sqlalchemy.Column('event_id',sqlalchemy.Integer()),
            sqlalchemy.Column('event_name',sqlalchemy.VARCHAR(50)),
            sqlalchemy.Column('event_date',sqlalchemy.DATE()),
            sqlalchemy.Column('event_type',sqlalchemy.VARCHAR(10)),
            sqlalchemy.Column('event_city',sqlalchemy.VARCHAR(20)),
            sqlalchemy.Column('customer_id',sqlalchemy.Integer()),
            sqlalchemy.Column('price',sqlalchemy.DECIMAL()),
            sqlalchemy.Column('num_tickets',sqlalchemy.Integer()),
        )
        
        # Execute statement to create table
        meta.create_all(connection)


        ### Read .CSV file into a pandas DF
        df = pd.read_csv(path,header=None)

        MiniPipeline.print_table()

        add_data = input(f"Add data from {filename} ?: Y/N: ")
        if add_data == "Y":

            ### Read .CSV file into a pandas DF
            df = pd.read_csv(path,header=None)

            # Iterate thru data, transforming as needed
            for i,row in df.iterrows():
                ticket_id = row[0]
                trans_date = datetime.datetime.strptime(row[1], "%m/%d/%Y").strftime("%Y-%m-%d")
                event_id = row[2]
                event_name = row[3]
                event_date = datetime.datetime.strptime(row[4], "%m/%d/%Y").strftime("%Y-%m-%d")
                event_type = row[5]
                event_city = row[6]
                customer_id = row[7]
                price = row[8]
                num_tickets = row[9]

                # Insert data into dataframe
                insert_stmt = sqlalchemy.insert(ticket_table).values(ticket_id = ticket_id,trans_date=trans_date,event_id=event_id,event_name=event_name,event_date=event_date,event_type=event_type,\
                    event_city=event_city,customer_id=customer_id,price=price,num_tickets=num_tickets)

                connection.execute(insert_stmt)

        else:
            print("No new data added")
            pass

        MiniPipeline.print_table()



    def query_popular_tickets():
        # Call function to get connection to MySQL
        connection = MiniPipeline.get_db_connection()

        # Get the most popular ticket in the past month
        sql_statement = "SELECT event_name, sum(num_tickets) as total_tickets FROM ticket_table GROUP BY event_id  ORDER BY total_tickets DESC LIMIT 3"
        stmt = connection.execute(sql_statement)
        records = stmt.fetchall()
        print('Here are the most popular tickets in the past month')
        for i in range(0,3,1):
            print(f'#{i+1}: {records[i][0]}')

        return records


    def print_table():
        # Call function to get connection to MySQL
        connection = MiniPipeline.get_db_connection()
        sql_statement = "SELECT * FROM ticket_table"
        stmt = connection.execute(sql_statement)
        records = stmt.fetchall()
        df = pd.DataFrame(records,columns=['ticket_id','trans_date','event_id','event_name','event_date','event_tyep','event_city','customer_id','price','num_tickets'])
        print(tabulate(df,headers='keys',tablefmt='fancy_grid'))


# Create instance of MiniPipeline class
instance = MiniPipeline

# Create table and load data in MySQL
instance.load_third_party(csv_path)

# run query to show results
instance.query_popular_tickets()