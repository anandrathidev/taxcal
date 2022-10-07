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
    finyear=2022
    syear = str.strip(str(finyear))
    syear_1 = str(int(str.strip(str(finyear))) - 1)
    dfinYearStart = datetime.strptime(f"30/Jun/{syear_1}", "%d/%b/%Y")
    dfinYearEnd = datetime.strptime(f"01/Jul/{syear}", "%d/%b/%Y")
    finYearStart = datetime.strftime(dfinYearStart, "%Y-%m-%d")
    finYearEnd = datetime.strftime(dfinYearEnd, "%Y-%m-%d")
    print(f"dfinYearStart {dfinYearStart}")
    print(f"finYearStart {finYearStart}")
    print(f"dfinYearEnd {dfinYearEnd}")
    print(f"finYearEnd {finYearEnd}")


    engine = create_engine('sqlite:///tax.db', echo=False)
    dfs.to_sql('transactions', con=engine, if_exists="replace")
    connection =  engine.connect()


    connection.execute(text("drop TABLE IF EXISTS TaxProfitTable" ))
    Createprofitabltext = text("""CREATE TABLE TaxProfitTable( Code TEXT,  Quantity INT,
                               BuyContractNote INT, SellContractNote INT, SellDate  INT, SaleUnitPrice REAL); """)
    connection.execute(Createprofitabltext)

    INSERTprofitabltext = text("""INSERT INTO TaxProfitTable(  Code, Quantity, BuyContractNote, SellContractNote, SellDate, SaleUnitPrice)
                               VALUES( :Code, :Quantity, :BuyContractNote, :SellContractNote, :SellDate, :SaleUnitPrice ); """)

    try:
        Sell_Text = text(""" SELECT
                         Code, Date, Type, Quantity, UnitPrice, TradeValue,
                         Brokerage_GST, GST, ContractNote, TotalValue
                        FROM transactions
                        WHERE Type = 'Sell'
                        AND
                        DATE(Date) >= DATE(:finYearStart)
                        AND
                        DATE(Date) <= DATE(:finYearEnd)
                        ORDER BY  UnitPrice DESC, Date DESC
                        """)
        Sell_Text_result = connection.execute(Sell_Text, finYearStart=finYearStart, finYearEnd=finYearEnd ).fetchall()
        print(Sell_Text_result)

        SelectBuytext =     text("""SELECT Code, Date, JULIANDAY(:sdate) - JULIANDAY(Date) as DayDiff,
                                  Type, Quantity, UnitPrice, TradeValue, Brokerage_GST, GST,
                                  TotalValue , ContractNote
                                  FROM transactions
                                  WHERE
                                  Code = :Code AND DATE(Date) < :sdate AND ContractNote <> :sellContractNote
                                  AND Type = "Buy"  ORDER BY  UnitPrice DESC, Date  DESC
                                  """)
        SelectProfitabltext = text("""SELECT Code, Quantity, BuyContractNote, SellContractNote, SellDate
                                   FROM TaxProfitTable
                                   WHERE Code = :Code  AND BuyContractNote = :BuyContractNote  """)

        #SellOldINSERTtext = text("""INSERT INTO Buymatchtbl select Code, Date,
        #                          JULIANDAY(:sdate) - JULIANDAY(Date) as YearDiff,  strftime('%Y', Date) as Year,
        #                          Type, Quantity, UnitPrice, TradeValue, Brokerage_GST, GST,
        #                          TotalValue from transactions WHERE Code = :Code and Type = 'Sell' and DATE(Date) < :finYearStart """)
        for row in Sell_Text_result:
            print("Code:", row['Code'] , "Date", row['Date'])
            #tresult = connection.execute(AllMatchtableText)
            #print("Buy  Code {} Count {}".format( row['Code'] , len(tresult.all())) )
            #_ = connection.execute(SellOldINSERTtext, Code=row['Code'], finYearStart=dfinYearStart , sdate=row['Date']  )
            Buy_Text_result = connection.execute(SelectBuytext, Code=row['Code'], sdate=row['Date'], sellContractNote=row["ContractNote"] )
            sellQty = abs(int(row["Quantity"]))
            for buyrow in Buy_Text_result:
                Sold_result = connection.execute(SelectProfitabltext, Code=row['Code'], BuyContractNote=buyrow["ContractNote"] )
                Sold_row = Sold_result.fetchone()
                if Sold_row:
                    SoldQty = abs(int(Sold_row["Quantity"]))
                else:
                    SoldQty = 0

                buyQty = abs(int(buyrow["Quantity"]))
                NetBuyQty = buyQty - SoldQty
                print(f"NetBuyQty={NetBuyQty} SoldQty={SoldQty} buyQty={buyQty} sellQty={sellQty} buyQty={buyQty} " )
                if NetBuyQty == 0:
                    continue
                if NetBuyQty < sellQty:
                    connection.execute(INSERTprofitabltext,
                            Code=row['Code'], Quantity=NetBuyQty,
                            BuyContractNote=buyrow["ContractNote"],
                            SellContractNote=row["ContractNote"],
                            SellDate=row['Date'],
                            SaleUnitPrice=row['UnitPrice']
                               )
                    sellQty = sellQty - NetBuyQty
                elif NetBuyQty > sellQty:
                    connection.execute(INSERTprofitabltext,
                            Code=row['Code'], Quantity=sellQty,
                            BuyContractNote=buyrow["ContractNote"],
                            SellContractNote=row["ContractNote"],
                            SellDate=row['Date'],
                            SaleUnitPrice=row['UnitPrice']
                               )
                    sellQty = 0
                    break

        ProfitLossTransactionJoin = """  SELECT
                                          Code,
                                          Date,
                                          Type,
                                          Quantity,
                                          UnitPrice,
                                          Quantity*UnitPrice as Amount,
                                          ContractNote,
                                          JULIANDAY(Date) - JULIANDAY(Date) AS Dtdifference,
                                          0 AS GainAmount,
                                          0 AS TaxableAmount
                                  FROM transactions
                                  WHERE
                                      transactions.Type = "Sell"
                                  AND
                                      DATE(Date) >= DATE(:finYearStart)
                                  AND
                                      DATE(Date) <= DATE(:finYearEnd)

                                  UNION

                                  SELECT
                                  TaxProfitTable.Code AS Code,
                                  transactions.Date AS Date,
                                  transactions.Type AS Type,
                                  TaxProfitTable.Quantity AS Quantity,
                                  transactions.UnitPrice AS UnitPrice,
                                  TaxProfitTable.Quantity*transactions.UnitPrice as Amount,
                                  transactions.ContractNote  AS ContractNote,
                                  JULIANDAY(TaxProfitTable.SellDate) - JULIANDAY(transactions.Date) AS Dtdifference,
                                  TaxProfitTable.Quantity * ( SaleUnitPrice - transactions.UnitPrice ) AS GainAmount,
                                CASE
                                WHEN  ( SaleUnitPrice - transactions.UnitPrice ) > 0.0  AND (JULIANDAY(TaxProfitTable.SellDate) - JULIANDAY(transactions.Date)) > 364 THEN 0.5* TaxProfitTable.Quantity * ( SaleUnitPrice - transactions.UnitPrice )
                                WHEN  ( SaleUnitPrice - transactions.UnitPrice ) > 0.0  AND (JULIANDAY(TaxProfitTable.SellDate) - JULIANDAY(transactions.Date)) <= 364 THEN TaxProfitTable.Quantity * ( SaleUnitPrice - transactions.UnitPrice )
                                WHEN  ( SaleUnitPrice - transactions.UnitPrice ) < 0.0  THEN TaxProfitTable.Quantity * ( SaleUnitPrice - transactions.UnitPrice )
                                END AS TaxableAmount
                                  FROM transactions, TaxProfitTable
                                  WHERE
                                          transactions.Code = TaxProfitTable.Code
                                      AND
                                          transactions.Type = "Buy"
                                      AND
                                          transactions.ContractNote = TaxProfitTable.BuyContractNote
                                    ORDER by Code , Date Desc
                                  """
        #tresult = connection.execute(ProfitLossTransactionJoin, finYearStart=finYearStart, finYearEnd=finYearEnd )
        table_df = pd.read_sql(ProfitLossTransactionJoin, con=engine, params={"finYearStart":finYearStart, "finYearEnd":finYearEnd })
        table_df['Date']= pd.to_datetime(table_df['Date'])

        table_df.to_csv(f"{syear}_ProfitLoss.csv", index_label="index", date_format="%Y-%m-%d", doublequote= True)
        table_df.groupby(['Code', 'Type']).agg({'Quantity': "sum", 'Amount': "sum",})
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
