# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import csv
from sqlalchemy import create_engine
from sqlalchemy.sql import text
engine = create_engine('sqlite:///tax.db', echo=False)
connection =  engine.connect()
connection.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                	"index" BIGINT,
                	"Code" TEXT,
                	"Date" DATETIME,
                	"Type" TEXT,
                	"Quantity" BIGINT,
                	"UnitPrice" FLOAT,
                	"TradeValue" FLOAT,
                	"TotalValue" FLOAT
                );
"""
)


def splitDetails(detstr, detDict):
    details = row[2].split()        
    if details[0] == "B":
        detDict["Type"] = "Buy"
    elif details[0] == "S":
        detDict["Type"] = "Sell"
    detDict["Qty"] = int(details[1].strip() )
    detDict["Code"] = details[2].strip()
    detDict["UnitPrice"] = float(details[4].strip())
        
def ToFloat(detDict, toflt):
    try:
        toflt = float(toflt)
    except:
        print(f"ERROR At {detDict}")
        toflt = detDict["Qty"] * detDict["UnitPrice"]
    return toflt    
            
user1 = {"id":100, "name": "Rumpelstiltskin", "dob": "12/12/12"}
with open('C:/Users/arathi/Downloads/Transactions.csv', 'r' ) as csvfile:
    csvrdr = csv.reader(csvfile, delimiter=',' )
    for row in csvrdr:
        detDict ={}
        try:
            detDict['Date'] = row[0].strip()
            detDict['Contract'] = row[1].strip()[1:]
            detstr = row[2].strip()
            if row[1].strip()[0] == 'C':
                splitDetails(detstr, detDict)
                if detDict["Type"] == "Buy":
                    detDict['TradeValue'] = ToFloat(detDict, row[3].strip() )
                elif detDict["Type"]  == "Sell":
                    detDict['TradeValue'] = ToFloat(detDict, row[4].strip() )
                    
                #print(f"Date: {detDict['Date']}  Contract: {detDict['Contract']} \
                #      Type: {detDict['Type']}  Qty: {detDict['Qty']}, \
                #      Code: {detDict['Code']} UnitPrice : {detDict['UnitPrice']} \
                #          TradeValue: {detDict['TradeValue']}  ")
        except Exception as e:
            print(f"ERROR At {detDict}")
            raise e