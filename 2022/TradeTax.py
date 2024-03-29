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
# import sqlite3
from sqlalchemy import create_engine
from sqlalchemy.sql import text

def readCSV(filename):
    try:
        df = pd.read_csv(filename)
    except Exception as e:
        print(e)
        return None
    return df


def trackSellandBuySQLGrouped( dfs , finyear, verbose):
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
        if verbose:
            print(f"Sales {Sell_Text_result}")

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
            if verbose:
                print( f" Sell Details : {row}" )
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
                    if verbose:
                        print(f" buy details ={buyrow} " )
                elif NetBuyQty > sellQty:
                    connection.execute(INSERTprofitabltext,
                            Code=row['Code'], Quantity=sellQty,
                            BuyContractNote=buyrow["ContractNote"],
                            SellContractNote=row["ContractNote"],
                            SellDate=row['Date'],
                            SaleUnitPrice=row['UnitPrice']
                               )
                    sellQty = 0
                    if verbose:
                        print(f" buy details ={buyrow} " )
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
                                          Brokerage_GST+GST AS Expenses,
                                          0.0 AS Taxable_GainLoss
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
                                  ((TaxProfitTable.Quantity* (Brokerage_GST+GST)) / transactions.Quantity) AS Expenses,
                                CASE
                                WHEN  ( SaleUnitPrice - transactions.UnitPrice ) > 0.0  AND (JULIANDAY(TaxProfitTable.SellDate) - JULIANDAY(transactions.Date)) > 364 THEN 0.5* TaxProfitTable.Quantity * ( SaleUnitPrice - transactions.UnitPrice )
                                WHEN  ( SaleUnitPrice - transactions.UnitPrice ) > 0.0  AND (JULIANDAY(TaxProfitTable.SellDate) - JULIANDAY(transactions.Date)) <= 364 THEN TaxProfitTable.Quantity * ( SaleUnitPrice - transactions.UnitPrice )
                                WHEN  ( SaleUnitPrice - transactions.UnitPrice ) < 0.0  THEN TaxProfitTable.Quantity * ( SaleUnitPrice - transactions.UnitPrice )
                                END AS Taxable_GainLoss
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
        print(f"Generating ... : {syear}_ProfitLoss.csv")
        table_df['Date']= pd.to_datetime(table_df['Date'])
        table_df.to_csv(f"{syear}_ProfitLoss.csv", index_label="index", date_format="%Y-%m-%d", doublequote= True)
        print(f"Successfuly Generated  : {syear}_ProfitLoss.csv")
        grp_df = table_df.groupby(['Code', 'Type'], as_index=False).agg({'Quantity': "sum", 'Amount': "sum", 'GainAmount': "sum", 'Expenses': "sum", 'Taxable_GainLoss': "sum" })
        grp_df.to_csv(f'{syear}_GroupProfitLoss.csv', index_label="index",)
        print(f"Successfuly Generated : {syear}_GroupProfitLoss.csv")
        print(f"GainAmount Negative is Loss , Positive is Profit")
        print(f"TaxableAmount  Negative is Loss , Positive is Profit")
        print(f"Quantity  Negative is Sell , Positive is Buy")
    except Exception as e:
        print(f"Exception is : {e} ")

def main():
    INPUTCSVFORMAT = """
    EXAMPLE Input  CSV Data format [Strictly ]

    First Line => Code,Company,Date,Type,Quantity,Unit Price ($),Trade Value ($),Brokerage+GST ($),GST ($),Contract Note,Total Value ($)
    Data Lines => XYZ, x Y Z co ,01/01/2001,Buy,50,1.00,50.00,10.0,1.0,221133.60
    Last  Line => MUST BE BLANK
    """
    print(INPUTCSVFORMAT)
    CLI=argparse.ArgumentParser(prog="TradeTax.exe", usage='%(prog)s [options]')
    CLI.add_argument("-v", action='store_true', default=False)
    CLI.add_argument(
      "--taxyear",  # name on the CLI - drop the `--` for positional/required parameters
      type=str,
      metavar="YYYY",
      required=True,
      default=date.today().year,  # default if nothing is provided
    )

    CLI.add_argument(
      "--data",  # name on the CLI - drop the `--` for positional/required parameters
      nargs="+",  # 1 or more values expected => creates a list
      type=str,
      metavar=" <file1.csv> <file2.csv> <file3.csv> ...",
      required=True,
      default=[],  # default if nothing is provided
    )
    CLI.print_help()
    # parse the command line
    args =  None
    try:
        args = CLI.parse_args()
    except Exception as e:
        print(f"Error : {e}")
        CLI.print_help()
        sys.exit(-1)

    VERBOSE = False
    if args.v:
        VERBOSE = True
    # access CLI options
    print("Tax Year: %r" % args.taxyear)
    print("list data files: %r" % args.data)
    taxYear = int(date.today().year)
    try:
        taxYear = int(args.taxyear)
    except Exception as e:
        print(e)
        sys.exit(-2)

    dfs = None

    for filename in args.data:
        print(f'Process : {filename}')
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
        trackSellandBuySQLGrouped( dfs=dfs , finyear=taxYear, verbose=VERBOSE)
    except Exception as e:
        print(f"trackSellandBuySQL {e}")

if __name__ == "__main__":
   main()
