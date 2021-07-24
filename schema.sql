drop database stocks;
create or replace  database stocks;
use  stocks;

CREATE OR REPLACE TABLE company (
Code VARCHAR(25) PRIMARY KEY, 
Company VARCHAR(2000)  NOT NULL,
Listingdate  DATE,
GICsindustrygroup VARCHAR(512),
MarketCap BIGINT UNSIGNED 
);

CREATE  OR REPLACE TABLE User (
UID BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE,
Name VARCHAR(512) ,
Postcode VARCHAR(512),
Country VARCHAR(512),
Email VARCHAR(2012),
Dob DATE
);


CREATE OR REPLACE TABLE trade (
UID BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE,
UserUid BIGINT UNSIGNED NOT NULL ,
Code VARCHAR(25) NOT NULL ,
Date DATE NOT NULL ,
Type VARCHAR(10) NOT NULL ,
Quantity  BIGINT UNSIGNED NOT NULL ,
UnitPrice  DOUBLE(16,5) UNSIGNED zerofill NOT NULL ,
TradeValue DOUBLE(16,5) UNSIGNED zerofill  NOT NULL ,
BrokerageGST DOUBLE(16,4) zerofill NOT NULL ,
GST DOUBLE(16,4) zerofill,
ContractNote VARCHAR(1000) NOT NULL ,
TotalValue DOUBLE,

CONSTRAINT `fk_trade_company`
    FOREIGN KEY (Code) REFERENCES company (Code),
CONSTRAINT `fk_trade_user`
    FOREIGN KEY (UserUid) REFERENCES User (UID)
);




CREATE OR REPLACE TABLE trademapping (
UID BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE,
UserUid BIGINT UNSIGNED NOT NULL ,
SellCode VARCHAR(25),
SellUID BIGINT UNSIGNED NOT NULL ,
BuyUID  BIGINT UNSIGNED NOT NULL ,
SellDate DATE,
BuyDate DATE,
CONSTRAINT `fk_trademapping_company`
    FOREIGN KEY (SellCode) REFERENCES company (Code),
CONSTRAINT `fk_trademapping_user`
    FOREIGN KEY (UserUid) REFERENCES User (UID),
CONSTRAINT `fk_trademapping_sell`
    FOREIGN KEY (SellUID) REFERENCES trade (UID),
CONSTRAINT `fk_trademapping_buy`
    FOREIGN KEY (BuyUID) REFERENCES trade (UID)
);




insert into User (Name , Postcode , Country , Email ) values ('Anand Rathi', '6011', 'Australia', '' );

LOAD DATA LOCAL INFILE 'C:/Users/a_rathi/LocalDocuments/IT/ASX_Listed_Companies_22-07-2021_03-01-26_AEST.csv' INTO TABLE stocks.company FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES  TERMINATED BY '\n' IGNORE 1 ROWS (Code, Company, Listingdate, GICsindustrygroup, MarketCap );

delete * from trade;

# LOAD DATA LOCAL INFILE 'C:/Users/a_rathi/LocalDocuments/IT/3105736_2019EOFYTransactions.csv' INTO TABLE stocks.trade FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES  TERMINATED BY  '\n' IGNORE 1 ROWS (@Code,@Company,@Date,@Type,@Quantity,@UnitPrice,@TradeValue,@BrokerageGST,@GST, @ContractNote, @TotalValue) set UserUid=1, Code=@Code , Date=STR_TO_DATE(@Date,'%d/%m/%y'),  Type=@Type,   Quantity=IF(@Quantity like '-%', @Quantity * -1, @Quantity), UnitPrice=@UnitPrice, TradeValue=IF(@TradeValue like '-%', @TradeValue * -1, @TradeValue) , BrokerageGST= @BrokerageGST,  GST=@GST,  ContractNote=@ContractNote,  TotalValue=IF(@TotalValue like '-%', @TotalValue * -1, @TotalValue) ;

#LOAD DATA LOCAL INFILE 'C:/Users/a_rathi/LocalDocuments/IT/3105736_2020EOFYTransactions.csv' INTO TABLE stocks.trade FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES  TERMINATED BY '\n' IGNORE 1 ROWS (@Code,@Company,@Date,@Type,@Quantity,@UnitPrice,@TradeValue,@BrokerageGST,@GST, @ContractNote, @TotalValue) set UserUid=1, Code=@Code , Date=STR_TO_DATE(@Date,'%d/%m/%y'),  Type=@Type,   Quantity=IF(@Quantity like '-%', @Quantity * -1, @Quantity), UnitPrice=@UnitPrice, TradeValue=IF(@TradeValue like '-%', @TradeValue * -1, @TradeValue) , BrokerageGST= @BrokerageGST,  GST=@GST,  ContractNote=@ContractNote,  TotalValue=IF(@TotalValue like '-%', @TotalValue * -1, @TotalValue) ;

#LOAD DATA LOCAL INFILE 'C:/Users/a_rathi/LocalDocuments/IT/3105736_2021EOFYTransactions.csv' INTO TABLE stocks.trade FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES  TERMINATED BY '\n' IGNORE 1 ROWS (@Code,@Company,@Date,@Type,@Quantity,@UnitPrice,@TradeValue,@BrokerageGST,@GST, @ContractNote, @TotalValue) set UserUid=1, Code=@Code , Date=STR_TO_DATE(@Date,'%d/%M/%y'),  Type=@Type,   Quantity=IF(@Quantity like '-%', @Quantity * -1, @Quantity), UnitPrice=@UnitPrice, TradeValue=IF(@TradeValue like '-%', @TradeValue * -1, @TradeValue) , BrokerageGST= @BrokerageGST,  GST=@GST,  ContractNote=@ContractNote,  TotalValue=IF(@TotalValue like '-%', @TotalValue * -1, @TotalValue) ;



select * from trade limit 10;


CREATE OR REPLACE TABLE calculate (
UserUid BIGINT UNSIGNED NOT NULL ,
Code VARCHAR(25),

SellUID BIGINT UNSIGNED NOT NULL ,
BuyUID  BIGINT UNSIGNED NOT NULL ,

SellDate DATE,
BuyDate DATE,

SellQty BIGINT UNSIGNED NOT NULL,
BuyQty BIGINT UNSIGNED NOT NULL,

SellUnitPrice  DOUBLE(16,5) UNSIGNED NOT NULL ,
BuyUnitPrice  DOUBLE(16,5) UNSIGNED  NOT NULL ,

SellTradeValue DOUBLE(16,5) UNSIGNED  NOT NULL ,
BuyTradeValue DOUBLE(16,5) UNSIGNED   NOT NULL ,

ProfitValue DOUBLE(16,5) UNSIGNED  NOT NULL ,
TaxValue DOUBLE(16,5) UNSIGNED   NOT NULL 

);


