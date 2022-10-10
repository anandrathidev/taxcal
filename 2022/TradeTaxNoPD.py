# -*- coding: utf-8 -*-
"""
Created on Sun Oct  2 20:05:30 2022

@author: a_rathi
"""

#import pandas as pd
import sys
import argparse
from pandas import read_csv
from pandas import to_numeric
from pandas import to_datetime
from pandas import read_sql
from pandas import concat
from datetime import date
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import csv

def readCSV(filename):
    try:
        df = read_csv(filename)
    except Exception as e:
        print(f"\n Error: read CSV {e}")
        return None
    return df

def writeCSV(filename, rows):
    try:
        #df = write_csv(filename)
        cols = []
        if len(rows) >0:
            cols = rows[0].keys()
        with open(filename, 'wb') as f:
            writer = csv.writer(f)
            writer.writerow(cols)
            writer.writerows(rows)

    except Exception as e:
        print(f"\n Error: read CSV {e}")

        return None
    return None

def trackSellandBuySQLGrouped( dfs , finyear, engine, verbose):
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


    connection =  engine.connect()

    CreateTransactiontext = text("""
                CREATE TABLE IF NOT EXISTS transactions (
                	"index" BIGINT,
                	"Code" TEXT,
                	"Company" TEXT,
                	"Date" DATETIME,
                	"Type" TEXT,
                	"Quantity" BIGINT,
                	"UnitPrice" FLOAT,
                	"TradeValue" FLOAT,
                	"Brokerage_GST" FLOAT,
                	"GST" FLOAT,
                	"ContractNote" BIGINT UNIQUE,
                	"TotalValue" FLOAT
                );
                               """)
    connection.execute(CreateTransactiontext)
    dfs.to_sql('transactions', con=engine, if_exists="append")

    #connection.execute(text("drop TABLE IF EXISTS TaxProfitTable" ))
    Createprofitabltext = text("""CREATE TABLE IF NOT EXISTS TaxProfitTable( Code TEXT,  Quantity INT,
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
        SelectProfitabltext = text("""SELECT Code, sum(Quantity) as Quantity
                                   FROM TaxProfitTable
                                   WHERE Code = :Code  AND BuyContractNote = :BuyContractNote
                                   GROUP BY Code, BuyContractNote
                                   """)

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
                Sold_rows = Sold_result.fetchall()
                if len(Sold_rows) >1:
                    print(f'Error: more than single Sold_rows: {Sold_rows}')
                elif len(Sold_rows) ==1:
                   Sold_row=Sold_rows[0]
                else:
                    Sold_row=None
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
                        print(f" buy details = {buyrow} " )
                elif NetBuyQty >= sellQty:
                    connection.execute(INSERTprofitabltext,
                            Code=row['Code'], Quantity=sellQty,
                            BuyContractNote=buyrow["ContractNote"],
                            SellContractNote=row["ContractNote"],
                            SellDate=row['Date'],
                            SaleUnitPrice=row['UnitPrice']
                               )
                    sellQty = 0
                    if verbose:
                        print(f" Buy details ={buyrow} " )
                    break

        ProfitLossTransactionJoin = """
            SELECT
			  Code,
			  Date,
			  Type,
              0 AS Typeord,
              0 AS Qty,
			  Quantity As SellQuantity,
			  0 As BuyQuantity,
			  UnitPrice,
			  Quantity*UnitPrice as Amount,
			  ContractNote,
			  JULIANDAY(Date) - JULIANDAY(Date) AS Dtdifference,
			  0.0 AS GainAmount,
			  Brokerage_GST+GST AS Expenses,
			  0.0 AS Taxable_GainLoss,
    		  0.0 AS SubTotalExpenses,
    		  0.0 AS SubTotalGainAmount,
    		  0.0 AS SubTotalTaxable_GainLoss
		  FROM transactions
		  WHERE
			  transactions.Type = "Sell"
		  AND
			  DATE(Date) >= DATE(:finYearStart)
		  AND
			  DATE(Date) <= DATE(:finYearEnd)

		 UNION

		  SELECT
		  NTaxProfitTable.Code AS Code,
		  transactions.Date AS Date,
		  transactions.Type AS Type,
          1 AS Typeord,
          0 AS Qty,
		  0 As SellQuantity,
		  NTaxProfitTable.Quantity AS BuyQuantity,
		  transactions.UnitPrice AS UnitPrice,
		  NTaxProfitTable.Quantity*transactions.UnitPrice as Amount,
		  transactions.ContractNote  AS ContractNote,
		  JULIANDAY(NTaxProfitTable.SellDate) - JULIANDAY(transactions.Date) AS Dtdifference,
		  NTaxProfitTable.Quantity * ( SaleUnitPrice - transactions.UnitPrice ) AS GainAmount,
		  ((NTaxProfitTable .Quantity* (Brokerage_GST+GST)) / transactions.Quantity) AS Expenses,
		CASE
		WHEN  ( SaleUnitPrice - transactions.UnitPrice ) > 0.0  AND (JULIANDAY(NTaxProfitTable.SellDate) - JULIANDAY(transactions.Date)) > 364 THEN 0.5* NTaxProfitTable.Quantity * ( SaleUnitPrice - transactions.UnitPrice )
		WHEN  ( SaleUnitPrice - transactions.UnitPrice ) > 0.0  AND (JULIANDAY(NTaxProfitTable.SellDate) - JULIANDAY(transactions.Date)) <= 364 THEN NTaxProfitTable.Quantity * ( SaleUnitPrice - transactions.UnitPrice )
		WHEN  ( SaleUnitPrice - transactions.UnitPrice ) < 0.0  THEN NTaxProfitTable.Quantity * ( SaleUnitPrice - transactions.UnitPrice )
		END AS Taxable_GainLoss,
		  0.0 AS SubTotalExpenses,
		  0.0 AS SubTotalGainAmount,
		  0.0 AS SubTotalTaxable_GainLoss
		FROM transactions,
		( select  * from TaxProfitTable
			WHERE SellContractNote in ( SELECT ContractNote FROM transactions WHERE transactions.Type = "Sell" AND DATE(Date) >= DATE(:finYearStart) AND DATE(Date) <= DATE(:finYearEnd) )
		) AS NTaxProfitTable
		  WHERE
				  transactions.Code = NTaxProfitTable.Code
			  AND
				  transactions.Type = "Buy"
			  AND
				  transactions.ContractNote = NTaxProfitTable.BuyContractNote
			ORDER by Code , Typeord , Date Desc

                                  """

        ProfitLossgroupJoin = """
        SELECT
        Code,
        min(Date) AS Date,
		'Totals' as Type,
        3 AS Typeord,
        SUM(SellQuantity) + Sum(BuyQuantity) as Qty,
        SUM(SellQuantity) as SellQuantity,
        SUM(BuyQuantity) as BuyQuantity,
		0.0 as UnitPrice,
		Sum(0.0) AS Amount,
		0 AS ContractNote,
		0 AS Dtdifference,
		0.0 AS GainAmount,
		Sum(0.0) AS Expenses,
		0.0 AS Taxable_GainLoss,
		SUM(Expenses) AS SubTotalExpenses,
		SUM(GainAmount) AS SubTotalGainAmount,
		SUM(Taxable_GainLoss)  AS SubTotalTaxable_GainLoss
        FROM ( """  + ProfitLossTransactionJoin + """ )
            AS ProfitLossTransactionJoin  GROUP BY Code
           UNION

        """  + ProfitLossTransactionJoin

        table_df = read_sql(ProfitLossgroupJoin, con=engine, params={"finYearStart":finYearStart, "finYearEnd":finYearEnd })
        print(f"Generating ... : {syear}_ProfitLoss.csv")
        table_df['Date']= to_datetime(table_df['Date'])
        table_df.to_csv(f"{syear}_ProfitLoss.csv", index_label="index", date_format="%Y-%m-%d", doublequote= True)
        print(f"Successfuly Generated  : {syear}_ProfitLoss.csv")

        ProfitLossGrandTot =  "SELECT * from (" + ProfitLossgroupJoin  + """) AS T2

            UNION

            SELECT
            'ZZZ_Total' AS Code,
            min(Date) AS Date,
    		'Grand Totals' as Type,
            4 AS Typeord,
            SUM(Qty) AS Qty,
            SUM(SellQuantity) AS SellQuantity,
            SUM(BuyQuantity) AS BuyQuantity,
    		0.0 AS UnitPrice,
    		Sum(0.0) AS Amount,
    		0 AS ContractNote,
    		0 AS Dtdifference,
    		Sum(GainAmount) AS GainAmount,
    		Sum(Expenses) AS Expenses,
    		Sum(Taxable_GainLoss) AS Taxable_GainLoss,
    		SUM(SubTotalExpenses) AS TotalExpenses,
    		SUM(SubTotalGainAmount) AS TotalGainAmount,
    		SUM(SubTotalTaxable_GainLoss)  AS TotalTaxable_GainLoss
            FROM (  """  + ProfitLossgroupJoin + " ) AS Tempt  ORDER by Code , Typeord , Date Desc "

        grp_df = read_sql(ProfitLossGrandTot, con=engine, params={"finYearStart":finYearStart, "finYearEnd":finYearEnd })

        grp_df.to_csv(f'{syear}_GrandTotalProfitLoss.csv', index_label="index",)
        print(f"Successfuly Generated : {syear}_GrandTotalProfitLoss.csv")
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
    print(f"list data files: {args.data}"  )
    taxYear = int(date.today().year)
    try:
        taxYear = int(args.taxyear)
    except Exception as e:
        print(f"\n Error: {e}")
        sys.exit(-2)

    dfs = None
    engine = create_engine('sqlite:///tax.db', echo=False)

    for filename in args.data:
        print(f'Process : {filename}')
        df = readCSV(filename)
        if VERBOSE==True:
            print(f'Read : {filename} ')
        if df is not None:
            try:
                df.columns = ['Code', 'Company', 'Date', 'Type', 'Quantity', 'UnitPrice', 'TradeValue', 'Brokerage_GST', 'GST', 'ContractNote', 'TotalValue']
                df[['Quantity', 'UnitPrice', 'TradeValue', 'Brokerage_GST', 'GST', 'ContractNote', 'TotalValue']] = df[['Quantity', 'UnitPrice', 'TradeValue', 'Brokerage_GST', 'GST', 'ContractNote', 'TotalValue']].apply(to_numeric)
                df['Date'] = to_datetime(df['Date'], dayfirst=True, infer_datetime_format=True)
            except Exception as e:
                print(e)
                sys.exit(-2)
            if dfs is None:
               dfs = df
            else:
                try:
                    dfs = concat([dfs, df])
                except Exception as e:
                    print(f"\n Error: {e}")
                    sys.exit(-2)


    try:
        trackSellandBuySQLGrouped( dfs=dfs , finyear=taxYear, engine=engine, verbose=VERBOSE)
    except Exception as e:
        print(f"trackSellandBuySQL {e}")

if __name__ == "__main__":
   main()
