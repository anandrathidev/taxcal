# -*- coding: utf-8 -*-
"""
Created on Thu Jul 22 23:01:10 2021

@author: a_rathi
"""

# Module Imports
import mariadb
import sys
import csv
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import MetaData, Table, Column, ForeignKey
from sqlalchemy import select
from sqlalchemy import insert
from dateutil.parser import parse
import datetime
def connectDB(user="root", password="anand123", host="127.0.0.1", port="3306", database="stocks" ):
    engine = None
    constr = f"mariadb+mariadbconnector://{user}:{password}!@{host}:{port}/{database}"   
    print(constr)
    try:
        engine = create_engine(constr)
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)
    return engine

def getTable(engine, tablename):
    meta = MetaData()
    meta.reflect(bind=engine)
    my_table = meta.tables[tablename]
    return my_table

   

def getAllTrades(UserID):
    # Connect to MariaDB Platform
    engine = connectDB()
    mtable = getTable(engine, tablename='trade')
    stmt = select(mtable).where(mtable.c.UserUid == UserID)
    # Print Result-set
    rows=[]
    with engine.connect() as conn:
        rs = conn.execute(stmt)
        rows = rs.mappings().all()
        for row in rows:
             print(row)
    return rows

def getAllBuyForSellTrades(UserID):
    # Connect to MariaDB Platform
    engine = connectDB()    
    
    with engine.connect() as con:
        rs = con.execute(
        """
        SELECT 
        UID ,
        UserUid ,
        Code ,
        Date as BuyDate ,
        Type ,
        Quantity ,
        TotalValue 
        FROM trade WHERE Type = 'Sell' and UserUid=? 
        Union 
        SELECT 
        Buy.UID ,
        Buy.UserUid ,
        Buy.Code ,
        Buy.Date ,
        Buy.Type ,
        Buy.Quantity  ,
        Buy.TotalValue
        FROM trade as Buy ,
        (
         SELECT 
        UID ,
        UserUid ,
        Code ,
        Date ,
        Type ,
        Quantity  ,
        TotalValue 
        FROM trade WHERE Type = 'Sell' and UserUid=? ) As Sell
        WHERE Buy.Type = 'Buy' and Buy.UserUid = Sell.UserUid and Buy.Code = Sell.Code and Buy.Date < Sell.Date;
 
        """, 
        (UserID,UserID,))
    
    # Print Result-set
    rows = rs.fetchall()
    #print("num of rows {} ".format(len(rows)))
    return rows

rows = getAllBuyForSellTrades(UserID=1)
rows

rows = getAllTrades(UserID=1)
rows
#for (UID,UserUid,Code ,Date , Type ,Quantity , TotalValue ) in rows: 
#        print(f"UID {UID} Code: {Code}, Date: {Date} , Type {Type} , Quantity {Quantity}, TotalValue {TotalValue}")
from sqlalchemy import func

def LoadData(csvfile, userid, dtformat = None):
    #con = connectDB()
    csvfile='C:\\Users\\a_rathi\\LocalDocuments\\IT\\3105736_2019EOFYTransactions.csv'
    df  = pd.read_csv(csvfile)
    dd = df.to_dict('records')
    file_len = len(dd)
    engine = connectDB()
    tradetable = getTable(engine, tablename='trade')
    company = getTable(engine, tablename='company')
    ifiles = getTable(engine, tablename='files')
    count_ = func.count('*')
    #tradetableClass = tradetable.__class__
    with Session(engine) as session:
        with session.begin():
            for csvline in dd:
                isCompanyExist = session.query(company.c.Code, count_).filter(company.c.Code == csvline['Code']).all()
                print(f".Code= {csvline['Code']}")
                print(f"isCompanyExist= {isCompanyExist}")
                if(isCompanyExist[0][1]<1): 
                    d = datetime.datetime(1980, 1, 1 )
                    print(f"INSERT = {isCompanyExist}")
                    insert_stmnt = company.insert().values(Code = csvline['Code'], 
                    Company = csvline['Company'], 
                    Listingdate = d,
                    GICsindustrygroup = "Unknown",
                    MarketCap = 0)
                    session.execute(insert_stmnt) 
    with Session(engine) as session:
        with session.begin():
            inscount = 0
            for csvline in dd:
                inscount = inscount+1
                isCompanyExist = session.query(company.c.Code, count_).filter(company.c.Code == csvline['Code']).all()
                print(f"isCompanyExist= {isCompanyExist}")
                if(isCompanyExist[0][1]<1): 
                    d = datetime.datetime(1980, 1, 1 )
                    company.insert().values(Code = csvline['Code'], 
                    Company = "", 
                    Listingdate = d,
                    GICsindustrygroup = "Unknown",
                    MarketCap = 0)
                
                csvline['Total Value ($)'] = int(csvline['Total Value ($)']) if  (int(csvline['Total Value ($)']) > 0) else  -1 * int(csvline['Total Value ($)'])
                csvline['Trade Value ($)'] = int(csvline['Trade Value ($)']) if  (int(csvline['Trade Value ($)']) > 0) else  -1 * int(csvline['Trade Value ($)'])
                csvline['Quantity'] = int(csvline['Quantity']) if  (int(csvline['Quantity']) > 0) else  -1 * int(csvline['Quantity'])
                insert_stmnt = tradetable.insert().values(
                    UserUid         = userid, 
                    Code            =       csvline['Code']              ,
                    Date 			= parse(csvline['Date'], dayfirst=True) if dtformat is None  else datetime.datetime.strptime(csvline['Date'], dtformat)   ,
                    Type 			=       csvline['Type']              ,
                    Quantity		=       csvline['Quantity']          ,
                    UnitPrice		=       csvline['Unit Price ($)']    ,
                    TradeValue		=       csvline['Trade Value ($)']   ,
                    BrokerageGST	=       csvline['Brokerage+GST ($)'] ,
                    GST				=       csvline['GST ($)']           ,
                    ContractNote	=       csvline['Contract Note']     ,
                    TotalValue		=       csvline['Total Value ($)']   
                    )
                session.execute(insert_stmnt)
            insert_stmnt = ifiles.insert().values(
                    UserUid = userid,
                    FileName = csvfile, 
                    Filecount = file_len,
                    Insertcount = inscount,
                    uploadDate = datetime.datetime.today()
                    )
            session.execute(insert_stmnt) 

LoadData(csvfile='C:\\Users\\a_rathi\\LocalDocuments\\IT\\3105736_2019EOFYTransactions.csv', userid=1)
LoadData(csvfile='C:\\Users\\a_rathi\\LocalDocuments\\IT\\3105736_2020EOFYTransactions.csv', userid=1)
LoadData(csvfile='C:\\Users\\a_rathi\\LocalDocuments\\IT\\3105736_2021EOFYTransactions.csv', userid=1)
 
