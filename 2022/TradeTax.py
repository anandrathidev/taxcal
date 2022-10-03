# -*- coding: utf-8 -*-
"""
Created on Sun Oct  2 20:05:30 2022

@author: a_rathi
"""

#import pandas as pd
import sys
import argparse
import pandas as pd
from datetime import date
from datetime import datetime
import sqlite3
from sqlalchemy import create_engine
from sqlalchemy.sql import text

def readCSV(filename):
    try:
        df = pd.read_csv(filename)
    except Exception as e:
        print(e)
        return None
    return df


def trackSellandBuySQLGrouped( dfs , finyear):
    syear = str.strip(str(finyear))
    syear_1 = str(int(str.strip(str(finyear))) - 1)
    dfinYearStart = datetime.strptime(f"30/Jun/{syear}", "%d/%b/%Y")
    dfinYearEnd = datetime.strptime(f"01/Jul/{syear_1}", "%d/%b/%Y")
    print(f"dfinYearStart {dfinYearStart}")
    engine = create_engine('sqlite://', echo=False)
    dfs.to_sql('transactions', con=engine)
    connection =  engine.connect()
    selltext = text("""select Code, Date, JULIANDAY(Date) - JULIANDAY(Date) > 364 as YearDiff,
                    strftime('%Y', Date) as Year, Type, Quantity, UnitPrice, TradeValue, Brokerage_GST,
                    GST, ContractNote, TotalValue from transactions where Type = :Sell and DATE(Date) BETWEEN DATE(:finYearStart) and DATE(:finYearEnd)""")
    resulttext = text("select * from matchtbl ORDER by Code")
    Groupresulttext = text("""select Code, Type, YearDiff,  sum(Quantity*UnitPrice) as Value ,
                           sum(Quantity) as Quantity
                           from matchtbl group by Code,  Type, YearDiff ORDER by Code,  Type, YearDiff""")
    createtext    =     text("""CREATE TABLE matchtbl AS select Code, Date(Date) as Date , JULIANDAY(Date) - JULIANDAY(Date) > 364 as YearDiff,
                             strftime('%Y', Date) as Year, Type, Quantity, UnitPrice, TradeValue, Brokerage_GST, GST,
                             TotalValue from transactions WHERE Type = 'Sell' and DATE(Date) >= DATE(:finYearStart) and DATE(Date) <= DATE(:finYearEnd)""")
    BUYINSERTtext =     text("""INSERT INTO matchtbl     select Code, Date, JULIANDAY(:sdate) - JULIANDAY(Date) > 364 as YearDiff,
                             strftime('%Y', Date) as Year, Type, Quantity, UnitPrice, TradeValue, Brokerage_GST, GST,
                             TotalValue from transactions WHERE Code = :Code and Type = 'Buy' and DATE(Date) < :sdate""")
    SellOldINSERTtext = text("""INSERT INTO matchtbl     select Code, Date,
                             JULIANDAY(:sdate) - JULIANDAY(Date) > 364 as YearDiff,  strftime('%Y', Date) as Year,
                             Type, Quantity, UnitPrice, TradeValue, Brokerage_GST, GST,
                             TotalValue from transactions WHERE Code = :Code and Type = 'Sell' and DATE(Date) < :finYearStart """)
    connection.execute(createtext, finYearStart=dfinYearStart, finYearEnd=dfinYearEnd )
    tresult = connection.execute(resulttext)
    print(" matchtbl First Count {} {}".format( len(tresult.all()), selltext) )

    try:
        result = connection.execute(selltext, Sell="Sell", finYearStart=dfinYearStart, finYearEnd=dfinYearEnd ).fetchall()
        print(result)
        for row in result:
            print("Code:", row['Code'] , "Year", row['Year'])
            _ = connection.execute(BUYINSERTtext, Code=row['Code'], sdate=row['Date'] )
            tresult = connection.execute(resulttext)
            print("Buy  Code {} Count {}".format( row['Code'] , len(tresult.all())) )
            _ = connection.execute(SellOldINSERTtext, Code=row['Code'], finYearStart=dfinYearStart , sdate=row['Date']  )
            tresult = connection.execute(resulttext)
            print("Sell  Code {} Count {}".format( row['Code'] , len(tresult.all())) )
            print(" matchtbl 1 Count {}".format( len(tresult.all())) )

        tresult = connection.execute(resulttext)
        print("Final Count {}".format( len(tresult.all()))  )
        table_df = pd.read_sql(resulttext, con=engine)
        table_df.to_csv(f"{syear}_Match.csv")

        tresult = connection.execute(Groupresulttext)
        print("Group Final Count {}".format( len(tresult.all()))  )
        table_df = pd.read_sql(Groupresulttext, con=engine)
        table_df.to_csv(f"{syear}_GroupMatch.csv")

    except Exception as e:
        print(f"Exception is : {e} ")


def main():
    CLI=argparse.ArgumentParser()
    CLI.add_argument(
      "--taxyear",  # name on the CLI - drop the `--` for positional/required parameters
      type=str,
      default=date.today().year,  # default if nothing is provided
    )

    CLI.add_argument(
      "--listFiles",  # name on the CLI - drop the `--` for positional/required parameters
      nargs="+",  # 1 or more values expected => creates a list
      type=str,
      default=[],  # default if nothing is provided
    )

    # parse the command line
    args = CLI.parse_args()
    # access CLI options
    print("Tax Year: %r" % args.taxyear)
    print("listFiles: %r" % args.listFiles)
    taxYear = int(date.today().year)
    try:
        taxYear = int(args.taxyear)
    except Exception as e:
        print(e)
        sys.exit(-2)

    dfs = None
    engine = create_engine('sqlite://', echo=True)

    for filename in args.listFiles:
        print(f'filename : {filename}')
        df = readCSV(filename)
        if df is not None:
            try:
                df.columns = ['Code', 'Company', 'Date', 'Type', 'Quantity', 'UnitPrice', 'TradeValue', 'Brokerage_GST', 'GST', 'ContractNote', 'TotalValue']
                df[['Quantity', 'UnitPrice', 'TradeValue', 'Brokerage_GST', 'GST', 'ContractNote', 'TotalValue']] = df[['Quantity', 'UnitPrice', 'TradeValue', 'Brokerage_GST', 'GST', 'ContractNote', 'TotalValue']].apply(pd.to_numeric)
                df['Date'] = pd.to_datetime(df['Date'], dayfirst=True, infer_datetime_format=True)
            except Exception as e:
                print(e)
                sys.exit(-2)
            if dfs is None:
               dfs = df
            else:
                try:
                    dfs = pd.concat([dfs, df])
                except Exception as e:
                    print(e)
                    sys.exit(-2)

    try:
        #trackSellandBuySQL( dfs , year=taxYear)
        trackSellandBuySQLGrouped( dfs=dfs , finyear=taxYear)
    except Exception as e:
        print(f"trackSellandBuySQL {e}")

if __name__ == "__main__":
   main()


"""
df = pd.read_csv("D:/LocalDocuments/IT/2_3105736_2022EOFYTransactions.csv")
df.columns = ['Code', 'Company', 'Date', 'Type', 'Quantity', 'UnitPrice', 'TradeValue', 'Brokerage_GST', 'GST', 'ContractNote', 'TotalValue']
df[['Quantity', 'UnitPrice', 'Trade Value', 'Brokerage_GST', 'GST', 'ContractNote', 'TotalValue']] = df[['Quantity', 'UnitPrice', 'TradeValue', 'Brokerage_GST', 'GST', 'ContractNote', 'TotalValue']].apply(pd.to_numeric)
df['Date'] =  pd.to_datetime(df['Date'], format='%d-%m-%Y')
pd.set_option('display.max_columns', None)
dtmask = df['Date'].dt.year == int("2022")
sellmask = df['Type'].str.contains("Sell")
sellyear = df[dtmask & sellmask]
print(sellyear)
print(sellyear.dtypes)

engine = create_engine('sqlite://', echo=False)
df.to_sql('transactions', con=engine)
connection =  engine.connect()
selltext = text("select Code, Company, strftime('%Y', Date) as Year, Type, Quantity, UnitPrice, TradeValue, Brokerage_GST, GST, ContractNote, TotalValue from transactions where Type = :Sell and Year = :yr")
resulttext = text("select * from matchtbl")
createtext    =     text("CREATE TABLE matchtbl AS   select Code, Company, strftime('%Y', Date) as Year, Type, Quantity , Quantity as SellQuantity, 0 as BuyQuantity,         UnitPrice, TradeValue, Brokerage_GST, GST, ContractNote, TotalValue from transactions WHERE  Type = 'Sell' and Year = :yr")
BUYINSERTtext =     text("INSERT INTO matchtbl select Code, Company, strftime('%Y', Date) as Year, Type, Quantity , 0 as SellQuantity,       Quantity as BuyQuantity,   UnitPrice, TradeValue, Brokerage_GST, GST, ContractNote, TotalValue from transactions WHERE Code = :Code and Type = 'Buy' and Year < :yr")
SellOldINSERTtext = text("INSERT INTO matchtbl select Code, Company, strftime('%Y', Date) as Year, Type, Quantity , Quantity as SellQuantity,       0 as BuyQuantity,   UnitPrice, TradeValue, Brokerage_GST, GST, ContractNote, TotalValue from transactions WHERE Code = :Code and Type = 'Sell' and Year < :yr")
connection.execute(createtext, yr=str(2022) )
# connection.execute(createtextNoYr)
tresult = connection.execute(resulttext)
print(" matchtbl 1 Count {}".format( len(tresult.all())) )

try:
    result = connection.execute(selltext, Sell="Sell", yr=str(2022) ).fetchall()
    #print(result)
    for row in result:
        print("Code:", row['Code'] , "Year", row['Year'])
        _ = connection.execute(BUYINSERTtext, Code=row['Code'], yr=str(2022) )
        tresult = connection.execute(resulttext)
        print("Buy  Code {} Count {}".format( row['Code'] , len(tresult.all())) )
        _ = connection.execute(SellOldINSERTtext, Code=row['Code'], yr=str(2022)  )
        tresult = connection.execute(resulttext)
        print("Sell  Code {} Count {}".format( row['Code'] , len(tresult.all())) )
        print(" matchtbl 1 Count {}".format( len(tresult.all())) )

    tresult = connection.execute(resulttext)
    print("Final Count {}".format( len(tresult.all()))  )
    table_df = pd.read_sql(resulttext, con=engine)
    table_df.to_csv("D:/LocalDocuments/IT/2022Match.csv")
except Exception as e:
    print(f"Exception is : {e} ")
"""
