"""
mpfi - Interface to interact with inforalgo's database.

Written by Sheng Chai and Alexandre Almosni   alexandre.almosni@gmail.com
(C) 2015-2016 Sheng Chai and Alexandre Almosni
Released under Apache 2.0 license. More info at http://www.apache.org/licenses/LICENSE-2.0


Defines the SQLTable class which is the representation of the Inforalgo table

Functions are self-explanatory.

Note that we shouldn't attempt to recreate the tables ourselves - this is dealt by Inforalgo people.

"""

import pandas
import datetime
import sqlalchemy
#from sqlalchemy import Column, CHAR, DATETIME, VARCHAR, TIMESTAMP
#from pandas.io import sql


##############
"""
Inputs specified by Inforalgo
"""
#bbrgDate = datetime.datetime.now()
#bbrgTime = datetime.datetime.now().strftime("%Y%m%d")
bbrgStatus       = 'completed'#THIS IS UPDATED IN THE DATABASE AFTER IT'S SENT BY INFORALGO
bbrgSend6        = 'Y'#THIS IS UPDATED IN THE DATABASE AFTER IT'S SENT BY INFORALGO
bbrgSend14       = 'N'
bbrgRectype      = '70'#was 43
bbrgSource       = 'NYCSBNY'
bbrgSectype      = '2'
bbrgConda        = '0'
bbrgCondb        = '0'
bbrgTranc        = ' '
bbrgCondc        = '0'
bbrgValc         = ' '
bbrgTrand        = ' '
bbrgCondd        = '0'
bbrgVald         = ' '
bbrgTrane        = ' '
bbrgConde        = '0'
bbrgVale         = ' '
bbrgTranf        = ' '
bbrgCondf        = '0'
bbrgValf         = ' '
bbrgTrang        = ' '
bbrgCondg        = '0'
bbrgValg         = ' '
bbrgTranh        = ' '
bbrgCondh        = '0'
bbrgValh         = ' '
bbrgTrani        = ' '
bbrgCondi        = '0'
bbrgVali         = ' '
bbrgTranj        = ' '
bbrgCondj        = '0'
bbrgValj         = ' '
bbrgTrank        = ' '
bbrgCondk        = '0'
bbrgValk         = ' '
bbrgTranl        = ' '
bbrgCondl        = '0'
bbrgVall         = ' '
bbrgOddlot       = ' '
bbrgCcyflag      = ' '
bbrgSourceid     = ' '
bbrgAcctype      = '08'
bbrgSecshrt      = ' '
bbrgAccbm        = ' '
bbrgBmsecid      = ' '
bbrgBmdesc       = ' '
bbrgFunct        = 'GDCO'
bbrgMonid        = ' '
bbrgLdind        = 'M'
bbrgAmdind       = 'M'
bbrgFrcol        = ' '
bbrgLdtype       = '02'
bbrgAbspg        = ' '
bbrgMono         = '0001'
bbrgMonpg        = ' '
bbrgOveride      = 'e' #This should be hex80, aka euro sign. #InforAlgo team is overriding this to the right symbol now
bbrgCmnt         = ' '
bbrgRow          = ' '
bbrgRsrv         = ' '
bbrgPackid       = ' '
bbrgYlwky        = ' '
bbrgSprice       = ' '
bbrgR2srv        = ' '
bbrgTmstamp      = ' '
bbrgSysnm        = None
bbrgUsernm       = None
bbrgOrigMonid    = None
bbrgOrigMono     = None
bbrgOrigAbsPg    = None
bbrgOrigRow      = None
bbrgLevel        = None
bbrgCompleteTime = None
bbrgPendTime     = None
bbrgExtractTime  = None
#################


class SQLTable():
    def __init__(self, bdm=None):
        self.bdm = bdm
        #Create sqlalchemy engine
        #connectionString = "mssql+pyodbc://InforAlgo_UAT"#"mssql+pyodbc://CIBLDNGSQLCU01C\GLOBALMC_UAT03/inftest?driver=SQL+Server+Native+Client+11.0?trusted_connection=yes"
        connectionString = 'mssql+pyodbc:///?odbc_connect=DRIVER%3D%7BSQL+Server%7D%3BDatabase%3Dinftest%3BSERVER%3DCIBLDNGSQLCU01C%5Cglobalmc_uat03'
        connectionStringPRD = 'mssql+pyodbc:///?odbc_connect=DRIVER%3D%7BSQL+Server%7D%3BDatabase%3Dinftest%3BSERVER%3DCIBLDNGAPPVP080%5Cprd03'
        self.engine = sqlalchemy.create_engine(connectionString, legacy_schema_aliasing=False)
        try:
            self.connection = self.engine.connect()
            print  'Connected to ' + connectionString
        except:
            print 'Connection to ' + connectionString + ' failed'
        pass

    def empty_table(self):
        m = sqlalchemy.MetaData()
        tblQuote = sqlalchemy.Table('tblQuote',m)
        self.connection.execute(tblQuote.delete())
        pass

    def delete_record(self, isin):
        #sql.execute("DELETE tblQuote WHERE bbrgSec6id='" + isin + "'", self.engine) ##THIS IS USING PANDAS
        self.connection.execute("DELETE tblQuote WHERE bbrgSec6id='" + isin + "'")


    def insert_record(self, isin, bid_price, ask_price, bid_size, ask_size):
        bbrgSec6id = isin
        bbrgSec14id = bbrgSec6id
        bbrgTrana = 'B'
        bbrgVala = bid_price
        bbrgTranb = 'Z'
        bbrgValb = str(bid_size)
        bbrgTranc = 'A'
        bbrgValc = ask_price
        bbrgTrand = 'Z'
        bbrgVald = str(ask_size)
        now = datetime.datetime.now()
        bbrgDate = now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        #bbrgTime = now.strftime('%H:%M:%S')
        bbrgTime = bbrgDate[11:19] #%H:%M:%S 20x faster than above
        bbrgInstance = 4
        cols = ['bbrgDate', 'bbrgTime', 'bbrgStatus', 'bbrgSend6', 'bbrgSend14', 'bbrgRectype', 'bbrgSource', 'bbrgSectype', 'bbrgSec6id', 'bbrgInstance', 'bbrgTrana', 'bbrgConda', 'bbrgVala', 'bbrgTranb', 'bbrgCondb', 'bbrgValb', 'bbrgTranc', 'bbrgCondc', 'bbrgValc', 'bbrgTrand', 'bbrgCondd', 'bbrgVald', 'bbrgTrane', 'bbrgConde', 'bbrgVale', 'bbrgTranf', 'bbrgCondf', 'bbrgValf', 'bbrgTrang', 'bbrgCondg', 'bbrgValg', 'bbrgTranh', 'bbrgCondh', 'bbrgValh', 'bbrgTrani', 'bbrgCondi', 'bbrgVali', 'bbrgTranj', 'bbrgCondj', 'bbrgValj', 'bbrgTrank', 'bbrgCondk', 'bbrgValk', 'bbrgTranl', 'bbrgCondl', 'bbrgVall', 'bbrgOddlot', 'bbrgCcyflag', 'bbrgSourceid', 'bbrgAcctype', 'bbrgSec14id', 'bbrgSecshrt', 'bbrgAccbm', 'bbrgBmsecid', 'bbrgBmdesc', 'bbrgFunct', 'bbrgMonid', 'bbrgLdind', 'bbrgAmdind', 'bbrgFrcol', 'bbrgLdtype', 'bbrgAbspg', 'bbrgMono', 'bbrgMonpg', 'bbrgCmnt', 'bbrgRow', 'bbrgRsrv', 'bbrgPackid', 'bbrgYlwky', 'bbrgSprice', 'bbrgR2srv', 'bbrgSysnm', 'bbrgUsernm', 'bbrgOrigMonid', 'bbrgOrigMono', 'bbrgOrigAbsPg', 'bbrgOrigRow', 'bbrgLevel']
        record = pandas.DataFrame(columns=cols)
        record.loc[0] = [bbrgDate, bbrgTime, bbrgStatus, bbrgSend6, bbrgSend14, bbrgRectype, bbrgSource, bbrgSectype, bbrgSec6id, bbrgInstance, bbrgTrana, bbrgConda, bbrgVala, bbrgTranb, bbrgCondb, bbrgValb, bbrgTranc, bbrgCondc, bbrgValc, bbrgTrand, bbrgCondd, bbrgVald, bbrgTrane, bbrgConde, bbrgVale, bbrgTranf, bbrgCondf, bbrgValf, bbrgTrang, bbrgCondg, bbrgValg, bbrgTranh, bbrgCondh, bbrgValh, bbrgTrani, bbrgCondi, bbrgVali, bbrgTranj, bbrgCondj, bbrgValj, bbrgTrank, bbrgCondk, bbrgValk, bbrgTranl, bbrgCondl, bbrgVall, bbrgOddlot, bbrgCcyflag, bbrgSourceid, bbrgAcctype, bbrgSec14id, bbrgSecshrt, bbrgAccbm, bbrgBmsecid, bbrgBmdesc, bbrgFunct, bbrgMonid, bbrgLdind, bbrgAmdind, bbrgFrcol, bbrgLdtype, bbrgAbspg, bbrgMono, bbrgMonpg, bbrgCmnt, bbrgRow, bbrgRsrv, bbrgPackid, bbrgYlwky, bbrgSprice, bbrgR2srv, bbrgSysnm, bbrgUsernm, bbrgOrigMonid, bbrgOrigMono, bbrgOrigAbsPg, bbrgOrigRow, bbrgLevel]#, bbrgCompleteTime, bbrgPendTime, bbrgExtractTime]
        record.to_sql('tblQuote',self.engine,schema='dbo',if_exists='append',index=False) 
        pass

    def send_price(self, isin, bid_price, ask_price, bid_size, ask_size):
        bbrgSec6id = isin
        bbrgTrana = 'B'
        bbrgVala = bid_price
        bbrgTranb = 'Z'
        bbrgValb = str(bid_size)
        bbrgTranc = 'A'
        bbrgValc = ask_price
        bbrgTrand = 'Z'
        bbrgVald = str(ask_size)
        now = datetime.datetime.now()
        bbrgDate = now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        #bbrgTime = now.strftime('%H:%M:%S')
        bbrgTime = bbrgDate[11:19] #%H:%M:%S 20x faster than above
        sqlfields = "bbrgTrana='{}', bbrgVala='{}', bbrgTranb='{}', bbrgValb='{}',bbrgTranc='{}', bbrgValc='{}', bbrgTrand='{}', bbrgVald='{}', bbrgDate='{}', bbrgTime='{}', bbrgStatus='{}', bbrgSend6='Y' WHERE bbrgSec6id='{}'".format(bbrgTrana,bbrgVala,bbrgTranb,bbrgValb,bbrgTranc,bbrgValc,bbrgTrand,bbrgVald,bbrgDate,bbrgTime, bbrgStatus, bbrgSec6id)
        #print "UPDATE tblQuote SET " + sqlfields
        #sql.execute("UPDATE tblQuote SET " + sqlfields, self.engine) ##THIS IS USING PANDAS
        self.connection.execute("UPDATE tblQuote SET " + sqlfields)
        pass

    def read_table(self):
        df = pandas.read_sql_table('tblQuote',self.engine,schema='dbo')
        return df[['bbrgDate','bbrgTime','bbrgStatus','bbrgSec6id','bbrgVala','bbrgValc','bbrgValb','bbrgVald']]

    # def start_of_day(self):
    #     try:#Read table - it could be empty
    #         df = pandas.read_sql_table('tblQuote',engine,schema='dbo')
    #         dt = datetime.datetime.now()
    #         dt = datetime.datetime(dt.year, dt.month, dt.day)
    #         if df['bbrgDate'].max() < dt:
    #             self.empty_table()
    #             #recreateRecords(bdm)
    #     except:
    #         pass






# def createTable():
#     '''
#     Creates a table with sqlalchemy based on the schema provided by inforalgo. 
#     This function shouldn't be used at all, because the table already exist...
#     '''
#     #Define table. Note:nullable=False => Field cannot be NULL!
#     m=sqlalchemy.MetaData()
#     tblQuote = sqlalchemy.Table('tblQuote',m,
#             Column('bbrgDate',DATETIME,nullable=False),
#             Column('bbrgTime',CHAR(8),nullable=False),
#             Column('bbrgStatus',CHAR(20),nullable=False),
#             Column('bbrgSend6',CHAR(1),nullable=False),
#             Column('bbrgSend14',CHAR(1),nullable=False),
#             Column('bbrgRectype',CHAR(2),nullable=False),
#             Column('bbrgSource',CHAR(7),nullable=False),
#             Column('bbrgSectype',CHAR(1),nullable=False),
#             Column('bbrgSec6id',CHAR(12),nullable=False),
#             Column('bbrgInstance',CHAR(2),nullable=False),
#             Column('bbrgTrana',CHAR(1),nullable=False),
#             Column('bbrgConda',CHAR(1),nullable=False),
#             Column('bbrgVala',CHAR(14),nullable=False),
#             Column('bbrgTranb',CHAR(1),nullable=False),
#             Column('bbrgCondb',CHAR(1),nullable=False),
#             Column('bbrgValb',CHAR(14),nullable=False),
#             Column('bbrgTranc',CHAR(1),nullable=False),
#             Column('bbrgCondc',CHAR(1),nullable=False),
#             Column('bbrgValc',CHAR(14),nullable=False),
#             Column('bbrgTrand',CHAR(1),nullable=False),
#             Column('bbrgCondd',CHAR(1),nullable=False),
#             Column('bbrgVald',CHAR(14),nullable=False),
#             Column('bbrgTrane',CHAR(1),nullable=False),
#             Column('bbrgConde',CHAR(1),nullable=False),
#             Column('bbrgVale',CHAR(14),nullable=False),
#             Column('bbrgTranf',CHAR(1),nullable=False),
#             Column('bbrgCondf',CHAR(1),nullable=False),
#             Column('bbrgValf',CHAR(14),nullable=False),
#             Column('bbrgTrang',CHAR(1),nullable=False),
#             Column('bbrgCondg',CHAR(1),nullable=False),
#             Column('bbrgValg',CHAR(14),nullable=False),
#             Column('bbrgTranh',CHAR(1),nullable=False),
#             Column('bbrgCondh',CHAR(1),nullable=False),
#             Column('bbrgValh',CHAR(14),nullable=False),
#             Column('bbrgTrani',CHAR(1),nullable=False),
#             Column('bbrgCondi',CHAR(1),nullable=False),
#             Column('bbrgVali',CHAR(14),nullable=False),
#             Column('bbrgTranj',CHAR(1),nullable=False),
#             Column('bbrgCondj',CHAR(1),nullable=False),
#             Column('bbrgValj',CHAR(14),nullable=False),
#             Column('bbrgTrank',CHAR(1),nullable=False),
#             Column('bbrgCondk',CHAR(1),nullable=False),
#             Column('bbrgValk',CHAR(14),nullable=False),
#             Column('bbrgTranl',CHAR(1),nullable=False),
#             Column('bbrgCondl',CHAR(1),nullable=False),
#             Column('bbrgVall',CHAR(14),nullable=False),
#             Column('bbrgOddlot',CHAR(1),nullable=False),
#             Column('bbrgCcyflag',CHAR(1),nullable=False),
#             Column('bbrgSourceid',CHAR(7),nullable=False),
#             Column('bbrgAcctype',CHAR(2),nullable=False),
#             Column('bbrgSec14id',CHAR(12),nullable=False),
#             Column('bbrgSecshrt',CHAR(14),nullable=False),
#             Column('bbrgAccbm',CHAR(2),nullable=False),
#             Column('bbrgBmsecid',CHAR(12),nullable=False),
#             Column('bbrgBmdesc',CHAR(20),nullable=False),
#             Column('bbrgFunct',CHAR(4),nullable=False),
#             Column('bbrgMonid',CHAR(4),nullable=False),
#             Column('bbrgLdind',CHAR(1),nullable=False),
#             Column('bbrgAmdind',CHAR(1),nullable=False),
#             Column('bbrgFrcol',CHAR(2),nullable=False),
#             Column('bbrgLdtype',CHAR(2),nullable=False),
#             Column('bbrgAbspg',CHAR(4),nullable=False),
#             Column('bbrgMono',CHAR(4),nullable=False),
#             Column('bbrgMonpg',CHAR(4),nullable=False),
#             Column('bbrgOverride',CHAR(4),nullable=False),#careful
#             Column('bbrgCmnt',CHAR(30),nullable=False),
#             Column('bbrgRow',CHAR(2),nullable=False),
#             Column('bbrgRsrv',CHAR(2),nullable=False),
#             Column('bbrgPackid',CHAR(2),nullable=False),
#             Column('bbrgYlwky',CHAR(1),nullable=False),
#             Column('bbrgSprice',CHAR(1),nullable=False),
#             Column('bbrgR2srv',CHAR(2),nullable=False),
#             Column('bbrgTmstamp',TIMESTAMP,nullable=True),
#             Column('bbrgSysnm',CHAR,nullable=True),
#             Column('bbrgUsernm',CHAR,nullable=True),
#             Column('bbrgOrigMonid',CHAR(4),nullable=True),
#             Column('bbrgOrigMono',CHAR(4),nullable=True),
#             Column('bbrgOrigAbsPg',CHAR(4),nullable=True),
#             Column('bbrgOrigRow',CHAR(2),nullable=True),
#             Column('bbrgLevel',VARCHAR(2),nullable=True),
#             schema='dbo'     
#         )



#     sqlalchemy.Index("tblQuote_IND1", tblQuote.c.bbrgLevel.asc(),tblQuote.c.bbrgMonid.asc(),tblQuote.c.bbrgMono.asc(),tblQuote.c.bbrgSec6id.asc(), mssql_clustered=True, unique=True)
#     #Create table
#     tblQuote.create(engine)


# def deleteTable(bdm):
#     """Function to delete records in the table at the start of day, then downloads bidSize information from BBG by calling 
#     downloadBidSizeInfo(). Function then calls fillData() to fill the grid.

#     Function is called by mpfiPricer.PricerWindow in line 1119.

#     Keyword argument:
#     bdm : bond data model.
#     """
#     #Delete table on start of day, then download bidsize info. 
#     #Then, fill in table!
#     df = pandas.read_sql('tblQuote',engine)
#     timenow = datetime.datetime.now().strftime("%Y%m%d")
#     if df['bbrgTime'].max() != timenow:
#         m = sqlalchemy.MetaData()
#         conn = engine.connect()
#         tblQuote = sqlalchemy.Table('tblQuote',m)
#         conn.execute(tblQuote.delete())
#         conn.close()
#         readTable()

#     bidSize = downloadBidSizeInfo(bdm.df)
#     data=fillData(bidSize)
#     data.to_csv('bidSize.csv')

# def deleteTableNew():
#     m = sqlalchemy.MetaData()
#     conn = engine.connect()
#     tblQuote = sqlalchemy.Table('tblQuote',m)
#     conn.execute(tblQuote.delete())
#     conn.close()


# def downloadBidSizeInfo(df):
#     """
#     Function is called by deleteTable() to download bid size information from Bloomberg.

#     Keyword argument:
#     df : pandas.DataFrame 
#     """

#     isinsList = df['ISIN'] + BBGHand+' Corp'
#     isinsList = list(isinsList.astype(str))

#     request = blpapiwrapper.BLPTS(isinsList,['BID_SIZE','BID','ASK'])
#     request.get()
#     bidSize = request.output.copy()
#     request.closeSession()
#     request = None
#     return bidSize 


# def fillData(bidSize):
#     """Extracts information from bidSize and sends BID_SIZE_DOWNLOAD event to fill the pricer grid. 
#     Function is called by deleteTable().

#     keyword argument:
#     bidSize : pandas.DataFrame that consists of downloaded Bid Size info.
#     """

#     #Fill bid size info in Pricing Grid
#     data=pandas.DataFrame(columns=['ISIN','bidSizeInfo','bidPrice','askPrice'])
#     for i in bidSize.index:
#         isin = i[0:12]
#         bondName = str(bonds[bonds['REGS']==isin].index.values[0])
#         bidSizeInfo = float(bidSize.loc[i].values[0])
#         bidPrice = float(bidSize.loc[i].values[1])
#         askPrice = float(bidSize.loc[i].values[2])
         
#         data.loc[bondName]=[isin,bidSizeInfo,bidPrice,askPrice]
#         pub.sendMessage('BID_SIZE_DOWNLOAD', data.loc[bondName])
    
#     return data 




# def populateTable(bbrgSec6id,bbrgTrana,bbrgVala,bbrgTranb,bbrgValb,bbrgTranc,bbrgValc,bbrgTrand,bbrgVald):
#     '''Populates pandas dataframe, then insert the row into the database table. Function is called by 

#     Function is called by Pricer.onEditCell()

#     Keyword arguments:

#     bbrgSec6id : ISIN of bond 
#     bbrgInstance : Number of fields to be published to bloomberg
#     bbrgTranc : hard code to 'a' for ask price. Space if no ask price to publish.
#     bbrgValc : ask price. Space if no ask price to publish.
#     bbrgTrand : hard code to 'Z' for ask size. Space if no ask size to publish.
#     bbrgVald : ask size. Space if no ask size to publish. 
#     bbrgTrana : hard code to 'b' for bid price. Space if no bid price to publish.
#     bbrgVala : bid price. Space if no bid price to publish.
#     bbrgTranb : hard code to 'Z' for bid size. Space if no bid size to publish.
#     bbrgValb : bid size. Space if no bid size to publish.
#     '''

#     now = datetime.datetime.now()
#     bbrgDate = now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
#     bbrgTime = now.strftime('%H:%M:%S')
    
# #    try:#if the row exists, we update it
#     sqlfields = "bbrgTrana='{}', bbrgVala='{}', bbrgTranb='{}', bbrgValb='{}',bbrgTranc='{}', bbrgValc='{}', bbrgTrand='{}', bbrgVald='{}', bbrgDate='{}', bbrgTime='{}', bbrgStatus='{}', bbrgSend6='Y' WHERE bbrgSec6id='{}'".format(bbrgTrana,bbrgVala,bbrgTranb,bbrgValb,bbrgTranc,bbrgValc,bbrgTrand,bbrgVald,bbrgDate,bbrgTime, bbrgStatus, bbrgSec6id)
#     sql.execute("UPDATE tblQuote SET "+sqlfields, engine)
#     #print "UPDATE tblQuote SET "+sqlfields
#     # except:#if the row doesn't exist, we create it - pandas.to_sql uses sqlalchemy to insert rows
#     #     bbrgSec14id = bbrgSec6id
#     #     bbrgInstance = 4#was 2
#     #     cols=['bbrgDate', 'bbrgTime', 'bbrgStatus', 'bbrgSend6', 'bbrgSend14', 'bbrgRectype', 'bbrgSource', 'bbrgSectype', 'bbrgSec6id', 'bbrgInstance', 'bbrgTrana', 'bbrgConda', 'bbrgVala', 'bbrgTranb', 'bbrgCondb', 'bbrgValb', 'bbrgTranc', 'bbrgCondc', 'bbrgValc', 'bbrgTrand', 'bbrgCondd', 'bbrgVald', 'bbrgTrane', 'bbrgConde', 'bbrgVale', 'bbrgTranf', 'bbrgCondf', 'bbrgValf', 'bbrgTrang', 'bbrgCondg', 'bbrgValg', 'bbrgTranh', 'bbrgCondh', 'bbrgValh', 'bbrgTrani', 'bbrgCondi', 'bbrgVali', 'bbrgTranj', 'bbrgCondj', 'bbrgValj', 'bbrgTrank', 'bbrgCondk', 'bbrgValk', 'bbrgTranl', 'bbrgCondl', 'bbrgVall', 'bbrgOddlot', 'bbrgCcyflag', 'bbrgSourceid', 'bbrgAcctype', 'bbrgSec14id', 'bbrgSecshrt', 'bbrgAccbm', 'bbrgBmsecid', 'bbrgBmdesc', 'bbrgFunct', 'bbrgMonid', 'bbrgLdind', 'bbrgAmdind', 'bbrgFrcol', 'bbrgLdtype', 'bbrgAbspg', 'bbrgMono', 'bbrgMonpg', 'bbrgCmnt', 'bbrgRow', 'bbrgRsrv', 'bbrgPackid', 'bbrgYlwky', 'bbrgSprice', 'bbrgR2srv', 'bbrgSysnm', 'bbrgUsernm', 'bbrgOrigMonid', 'bbrgOrigMono', 'bbrgOrigAbsPg', 'bbrgOrigRow', 'bbrgLevel']#, 'bbrgCompleteTime', 'bbrgPendTime', 'bbrgExtractTime']
#     #     mpfidf=pandas.DataFrame(columns=cols)
#     #     mpfidf.loc[0]=[bbrgDate, bbrgTime, bbrgStatus, bbrgSend6, bbrgSend14, bbrgRectype, bbrgSource, bbrgSectype, bbrgSec6id, bbrgInstance, bbrgTrana, bbrgConda, bbrgVala, bbrgTranb, bbrgCondb, bbrgValb, bbrgTranc, bbrgCondc, bbrgValc, bbrgTrand, bbrgCondd, bbrgVald, bbrgTrane, bbrgConde, bbrgVale, bbrgTranf, bbrgCondf, bbrgValf, bbrgTrang, bbrgCondg, bbrgValg, bbrgTranh, bbrgCondh, bbrgValh, bbrgTrani, bbrgCondi, bbrgVali, bbrgTranj, bbrgCondj, bbrgValj, bbrgTrank, bbrgCondk, bbrgValk, bbrgTranl, bbrgCondl, bbrgVall, bbrgOddlot, bbrgCcyflag, bbrgSourceid, bbrgAcctype, bbrgSec14id, bbrgSecshrt, bbrgAccbm, bbrgBmsecid, bbrgBmdesc, bbrgFunct, bbrgMonid, bbrgLdind, bbrgAmdind, bbrgFrcol, bbrgLdtype, bbrgAbspg, bbrgMono, bbrgMonpg, bbrgCmnt, bbrgRow, bbrgRsrv, bbrgPackid, bbrgYlwky, bbrgSprice, bbrgR2srv, bbrgSysnm, bbrgUsernm, bbrgOrigMonid, bbrgOrigMono, bbrgOrigAbsPg, bbrgOrigRow, bbrgLevel]#, bbrgCompleteTime, bbrgPendTime, bbrgExtractTime]
#     #     mpfidf.to_sql('tblQuote',engine,schema='dbo',if_exists='append',index=False) 

# def send_price(bbg_sec_id, bid_price, ask_price, bid_size, ask_size):
#     populateTable(bbg_sec_id, 'B', bid_price, 'Z', bid_size, 'A', ask_price, 'Z', ask_size)

# def stop_price(bbg_sec_id):
#     now = datetime.datetime.now()
#     bbrgDate = now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
#     bbrgTime = now.strftime('%H:%M:%S')
#     bbrgSec6id = bbg_sec_id
#     bbrgTrana = 'B'
#     bbrgVala = 100
#     bbrgTranb = 'Z'
#     bbrgValb = 1000000
#     bbrgTranc = 'A'
#     bbrgValc = 100
#     bbrgTrand = 'Z'
#     bbrgVald = 1000000
#     # try:#if the row exists, we update it
#     sqlfields = "bbrgStatus='{}', bbrgLdind='D', bbrgAmdind='D', bbrgSend6='Y' WHERE bbrgSec6id='{}'".format(bbrgStatus, bbrgSec6id)
#     sql.execute("UPDATE tblQuote SET "+sqlfields, engine)
#     #print "UPDATE tblQuote SET "+sqlfields
#     # except:#if the row doesn't exist, we create it - pandas.to_sql uses sqlalchemy to insert rows
#     #     bbrgSec14id = bbrgSec6id
#     #     bbrgInstance = 4#was 2
#     #     cols=['bbrgDate', 'bbrgTime', 'bbrgStatus', 'bbrgSend6', 'bbrgSend14', 'bbrgRectype', 'bbrgSource', 'bbrgSectype', 'bbrgSec6id', 'bbrgInstance', 'bbrgTrana', 'bbrgConda', 'bbrgVala', 'bbrgTranb', 'bbrgCondb', 'bbrgValb', 'bbrgTranc', 'bbrgCondc', 'bbrgValc', 'bbrgTrand', 'bbrgCondd', 'bbrgVald', 'bbrgTrane', 'bbrgConde', 'bbrgVale', 'bbrgTranf', 'bbrgCondf', 'bbrgValf', 'bbrgTrang', 'bbrgCondg', 'bbrgValg', 'bbrgTranh', 'bbrgCondh', 'bbrgValh', 'bbrgTrani', 'bbrgCondi', 'bbrgVali', 'bbrgTranj', 'bbrgCondj', 'bbrgValj', 'bbrgTrank', 'bbrgCondk', 'bbrgValk', 'bbrgTranl', 'bbrgCondl', 'bbrgVall', 'bbrgOddlot', 'bbrgCcyflag', 'bbrgSourceid', 'bbrgAcctype', 'bbrgSec14id', 'bbrgSecshrt', 'bbrgAccbm', 'bbrgBmsecid', 'bbrgBmdesc', 'bbrgFunct', 'bbrgMonid', 'bbrgLdind', 'bbrgAmdind', 'bbrgFrcol', 'bbrgLdtype', 'bbrgAbspg', 'bbrgMono', 'bbrgMonpg', 'bbrgCmnt', 'bbrgRow', 'bbrgRsrv', 'bbrgPackid', 'bbrgYlwky', 'bbrgSprice', 'bbrgR2srv', 'bbrgSysnm', 'bbrgUsernm', 'bbrgOrigMonid', 'bbrgOrigMono', 'bbrgOrigAbsPg', 'bbrgOrigRow', 'bbrgLevel']#, 'bbrgCompleteTime', 'bbrgPendTime', 'bbrgExtractTime']
#     #     mpfidf=pandas.DataFrame(columns=cols)
#     #     mpfidf.loc[0]=[bbrgDate, bbrgTime, bbrgStatus, bbrgSend6, bbrgSend14, bbrgRectype, bbrgSource, bbrgSectype, bbrgSec6id, bbrgInstance, bbrgTrana, bbrgConda, bbrgVala, bbrgTranb, bbrgCondb, bbrgValb, bbrgTranc, bbrgCondc, bbrgValc, bbrgTrand, bbrgCondd, bbrgVald, bbrgTrane, bbrgConde, bbrgVale, bbrgTranf, bbrgCondf, bbrgValf, bbrgTrang, bbrgCondg, bbrgValg, bbrgTranh, bbrgCondh, bbrgValh, bbrgTrani, bbrgCondi, bbrgVali, bbrgTranj, bbrgCondj, bbrgValj, bbrgTrank, bbrgCondk, bbrgValk, bbrgTranl, bbrgCondl, bbrgVall, bbrgOddlot, bbrgCcyflag, bbrgSourceid, bbrgAcctype, bbrgSec14id, bbrgSecshrt, bbrgAccbm, bbrgBmsecid, bbrgBmdesc, bbrgFunct, bbrgMonid, 'D', 'D', bbrgFrcol, bbrgLdtype, bbrgAbspg, bbrgMono, bbrgMonpg, bbrgCmnt, bbrgRow, bbrgRsrv, bbrgPackid, bbrgYlwky, bbrgSprice, bbrgR2srv, bbrgSysnm, bbrgUsernm, bbrgOrigMonid, bbrgOrigMono, bbrgOrigAbsPg, bbrgOrigRow, bbrgLevel]#, bbrgCompleteTime, bbrgPendTime, bbrgExtractTime]
#     #     print mpfidf
#     #     mpfidf.to_sql('tblQuote',engine,schema='dbo',if_exists='append',index=False) 
#     # pass




