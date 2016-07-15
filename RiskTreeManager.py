"""
Tree display of Front risk
Written by Alexandre Almosni   alexandre.almosni@gmail.com
(C) 2014-2016 Alexandre Almosni
Released under Apache 2.0 license. More info at http://www.apache.org/licenses/LICENSE-2.0

Functions:
tradeVolume()
zeroCondition()

Class:
RiskTreeManager
"""

import  wx
import pandas
import datetime
import threading
import time
import pythoncom
import win32api
import win32print
import os
from wx.lib.pubsub import pub

import blpapiwrapper
from StaticDataImport import ccy, countries, bonds, BBGHand, TEMPPATH, isinsregs, SPECIALBONDS, allisins

todayDateSTR=datetime.datetime.today().strftime('%d/%m/%y')

def tradeVolume(th,key,item):
    return th.df[(th.df[key]==item) & (th.df['Date']==todayDateSTR)]['Qty'].sum()


class MessageContainer():
    def __init__(self,data):
        self.data=data

class BondPriceUpdateMessage():
    def __init__(self,bond,booklist,price):
        self.bond = bond
        self.booklist = booklist
        self.price = price

#def zeroCondition(th,key,item):
#    nopos = (th.positions[th.positions[key]==item]['Qty'].min()>=-1 and th.positions[th.positions[key]==item]['Qty'].max()<=1)
#    notrades=len(th.df[(th.df[key]==item) & (th.df['Date']==todayDateSTR)])==0
#    return (notrades and nopos)


#----------------------------------------------------------------------

class RiskTreeManager():
    """Class to define the Risk Tree Panel 

    Attributes:

    Methods:
    __init__()
    OnActivate()
    onCollapseAll()
    onRiskTreeQuery()
    OnRightUp()
    OnSize()
    onFillEODPrices()
    onUpdateTree()
    takeScreenshot()

    """
    def __init__(self, th, parent):
        """Keyword arguments:
        parent : parent 
        th = trade history (defaults to empty array if not specified)
        """
        self.th = th
        self.parent = parent
        self.EODPricesFilled = False
        self.LivePricesFilled = False
        self.bdmReady = False
        self.lock = threading.Lock()
        #self.onUpdateTree()
        #FIRST BUILD BELOW
        self.firstBuild = True
        self.cntrymap = countries.set_index('Country code')
        #RISK TREE
        self.th.positions['EODPrice'] = 0
        self.th.positions['EODValue'] = 0
        self.th.positions['Risk'] = 0
        self.displayPositions=self.th.positions[(self.th.positions['Qty']<=-1) | (self.th.positions['Qty']>=1)].copy()
        self.displayPositions=self.displayPositions.join(bonds['REGS'])
        self.displayPositions = self.displayPositions.join(self.cntrymap['Long name'],on='Country')
        self.displayPositions.rename(columns={'Long name':'LongCountry'},inplace=True)
        self.displayGroup = self.displayPositions.groupby(['Region','LongCountry','Issuer','Bond']).sum()
        #BOOK AND PnL TREE
        #self.th.positionsByISINBook=self.th.positionsByISINBook
        self.th.positionsByISINBook['Qty']=self.th.positionsByISINBook['SOD_Pos']#Qty will be current, SOD is start of day
        for c in ['EODPrice','EODValue','PriceY','Risk','USDQty','PriceT','SODPnL','TradePnL','TotalPnL','MK']:
            self.th.positionsByISINBook[c] = 0
        self.th.positionsByISINBook = self.th.positionsByISINBook.join(self.cntrymap['Long name'],on='Country')
        self.th.positionsByISINBook.rename(columns={'Long name':'LongCountry'},inplace=True)
        self.th.positionsByISINBook.set_index('Key',inplace=True)
        self.displayGroupBook = self.th.positionsByISINBook.groupby(['Book','LongCountry','Issuer','Bond','Series']).sum()
        self.traded_bonds = []
        self.firstBuild = False
        pub.subscribe(self.updatePrice, "BOND_PRICE_UPDATE")
        pub.subscribe(self.switchBDMReady, "BDM_READY")
        pub.subscribe(self.onUpdateTree, "POSITION_UPDATE")
        pass

    def switchBDMReady(self, message):
        self.bdm = message.data
        self.bdmReady = True
        self.treeRebuild()
        pass

    def updatePrice(self, message):
        self.lock.acquire()
        series = message.data
        bond = series.name
        price = series['MID']
        idx = self.th.positionsByISINBook['Bond']==bond
        self.th.positionsByISINBook.loc[idx,'PriceT'] = price
        self.th.positionsByISINBook.loc[idx,'SODPnL'] = self.th.positionsByISINBook.loc[idx,'SOD_Pos'] * self.th.positionsByISINBook.loc[idx,'PRINCIPAL_FACTOR'] * (price - self.th.positionsByISINBook.loc[idx,'PriceY'])/100.
        self.th.positionsByISINBook.loc[idx,'SODPnL'].fillna(0, inplace=True)
        fx = ccy.at[bonds.at[bond,'CRNCY'],'2016']
        # keylist=[]
        # booklist=[]
        if bond in self.new_trades['Bond'].values:
            for (k,grp) in self.positionDeltas:
                isin=k[1]
                # isinlist.append(isin)
                # booklist.append(k[0])
                try:
                    if allisins[isin]==bond:#grp['Qty'].sum()!=0 
                        idx = (self.new_trades['ISIN']==isin) & (self.new_trades['Book']==k[0])
                        #print isin, price
                        self.new_trades.loc[idx,'TradePnL']=self.new_trades.loc[idx,'Qty']*(price-self.new_trades.loc[idx,'Price'])/100.
                        self.th.positionsByISINBook.at[k[0]+'-'+k[1],'TradePnL'] = self.th.positionsByISINBook.at[k[0]+'-'+k[1],'PRINCIPAL_FACTOR'] * self.new_trades.loc[idx,'TradePnL'].sum()
                except:
                    #bond is dummy
                    pass
        ########
        bondlines = self.th.positionsByISINBook['Bond']==bond
        self.th.positionsByISINBook.loc[bondlines,'TotalPnL']=self.th.positionsByISINBook.loc[bondlines,'SODPnL']/fx + self.th.positionsByISINBook.loc[bondlines,'TradePnL']/fx
        booklist=list(self.th.positionsByISINBook.loc[bondlines,'Book'].drop_duplicates())
        message=BondPriceUpdateMessage(bond=bond, booklist=booklist, price=price)
        pub.sendMessage('RISKTREE_BOND_PRICE_UPDATE', message=message)
        #we send ISIN, bond, price
        #update RiskTreeView
        self.lock.release()
        pass



    def onFillEODPrices(self, fc):
        """Function to download EOD Prices from Front, Principal from Bloomberg and calculate PV.
        This will check whether a file was already saved today with eod prices.
        This is only done on firstBuild.

        Keyword argument:
        fc : front connection FO_Toolkit > FrontConnection class instance
        """


        #Retrieve eod prices
        savepath=TEMPPATH+'EODPrices.csv'
        noEODPricesFile = not(os.path.exists(savepath)) or datetime.datetime.fromtimestamp(os.path.getmtime(savepath)).date()<datetime.datetime.today().date()
        self.fc = fc
        #self.th.positions['EODPrice']=0.0
        #self.th.positions['EODValue']=0.0
        #traded_bonds=self.th.positions[(self.th.positions['Qty']<=-1) | (self.th.positions['Qty']>=1)]['Bond']
        if noEODPricesFile:
            #########################################
            #Retrieve EOD prices
            _offsets = (3, 1, 1, 1, 1, 1, 2)
            yesterday = (datetime.datetime.today() - datetime.timedelta(days=_offsets[datetime.datetime.today().weekday()])).strftime('%Y-%m-%d')
            #self.th.positionsByISINBook['PriceY']=0.0
            for idx,row in self.th.positionsByISINBook.iterrows():
                self.th.positionsByISINBook.loc[idx,'PriceY']=self.fc.historical_price_query(row['ISIN'], yesterday)
            #print self.th.positionsByISINBook    
            self.th.positionsByISINBook.to_csv(TEMPPATH+'SOD_risk_prices.csv')
            for bond in self.displayPositions.index:
                self.th.positions.loc[bond,'EODPrice']=self.th.positionsByISINBook.loc[self.th.positionsByISINBook['Bond']==bond,'PriceY'].iloc[0]
            self.EODPrices = self.th.positions['EODPrice'].copy()
            self.EODPrices.to_csv(savepath)
            #########################################
            #Retrieve principal factor and SPV01 for positions
            inputdic = (self.displayPositions['REGS'] + ' Corp').to_dict()
            out = blpapiwrapper.simpleReferenceDataRequest(inputdic,['PRINCIPAL_FACTOR','RISK_MID'])
            #out = out.astype(float)
            self.principalFactor = out['PRINCIPAL_FACTOR'].astype(float).dropna()
            self.riskMid = out['RISK_MID'].astype(float).dropna()
            #########################################
            #Now need to deal with special bonds
            spbonds = list(set(SPECIALBONDS) & set(self.displayPositions.index))
            if len(spbonds)>0:
                dc=dict(zip(spbonds,map(lambda x:bonds.loc[x,'REGS']+ ' Corp',spbonds)))
                output=blpapiwrapper.simpleReferenceDataRequest(dc,['WORKOUT_OAS_MID_MOD_DUR'])
                self.th.positions.loc[spbonds,'RISK_MID']=output['WORKOUT_OAS_MID_MOD_DUR'].astype(float)
                self.riskMid[spbonds]=self.th.positions.loc[spbonds, 'RISK_MID']

            self.principalFactor.to_csv(TEMPPATH+'principalFactor.csv')
            self.riskMid.to_csv(TEMPPATH+'riskMid.csv')            
            self.principalFactor.name = 'PRINCIPAL_FACTOR'
            self.riskMid.name = 'RISK_MID'
            self.EODPrices.name = 'EODPrice'

        else:
            # self.principalFactor = pandas.Series.from_csv(TEMPPATH+'principalFactor.csv')
            # self.riskMid = pandas.Series.from_csv(TEMPPATH+'riskMid.csv')
            # self.EODPrices = pandas.Series.from_csv(savepath)
            self.principalFactor = pandas.read_csv(TEMPPATH+'principalFactor.csv',header=None,index_col=0,names=['PRINCIPAL_FACTOR'],squeeze=True)
            self.riskMid = pandas.read_csv(TEMPPATH+'riskMid.csv',header=None,index_col=0,names=['RISK_MID'],squeeze=True)
            self.EODPrices = pandas.read_csv(savepath,header=None,index_col=0,names=['EODPrice'],squeeze=True)

            self.th.positions['EODPrice'] = self.EODPrices
            self.th.positionsByISINBook = pandas.read_csv(TEMPPATH+'SOD_risk_prices.csv', index_col=0) #to pick up the prices
        #print self.riskMid
        self.th.positions['EODValue'] = self.th.positions['USDQty']*self.th.positions['EODPrice']/100.*(self.principalFactor)
        self.th.positions['Risk'] = -self.th.positions['USDQty']*self.riskMid/10000
        self.th.positions.loc[self.th.positions['Issuer']=='T','Risk'] = 0 # UST HAVE NO CREDIT RISK
        self.th.positions.loc[self.th.positions['Issuer']=='DBR','Risk'] = 0 # BUNDS HAVE NO CREDIT RISK
        
        del self.th.positionsByISINBook['EODPrice']
        self.th.positionsByISINBook = self.th.positionsByISINBook.join(self.EODPrices, on='Bond')
        self.th.positionsByISINBook['USDQty'] = self.th.positionsByISINBook.apply(lambda row:row['Qty']/ccy.loc[row['CCY'],'2016'],axis=1)
        self.th.positionsByISINBook = self.th.positionsByISINBook.join(self.principalFactor, on='Bond')
        self.th.positionsByISINBook['EODValue'] = self.th.positionsByISINBook['EODPrice']*self.th.positionsByISINBook['USDQty']/100.*(self.th.positionsByISINBook['PRINCIPAL_FACTOR'])
        self.th.positionsByISINBook = self.th.positionsByISINBook.join(self.riskMid, on = 'Bond')
        self.th.positionsByISINBook['Risk'] = -self.th.positionsByISINBook['USDQty']*self.th.positionsByISINBook['RISK_MID']/10000
        self.th.positionsByISINBook.loc[self.th.positionsByISINBook['Issuer']=='T','Risk'] = 0 # UST HAVE NO CREDIT RISK
        self.th.positionsByISINBook.loc[self.th.positionsByISINBook['Issuer']=='DBR','Risk'] = 0 # BUNDS HAVE NO CREDIT RISK

        #Now rebuild trees
        self.EODPricesFilled=True
        #print self.th.positionsByISINBook
        self.treeRebuild()


    def onUpdateTree(self, message=None):
        '''EVENT LISTENER
        '''
        self.treeRebuild()

    def treeRebuild(self):
        #pythoncom.CoInitialize()
        _offsets = (3, 1, 1, 1, 1, 1, 2)
        yesterday = (datetime.datetime.today() - datetime.timedelta(days=_offsets[datetime.datetime.today().weekday()])).strftime('%Y-%m-%d')
        self.traded_bonds = self.th.df[self.th.df['Date']==todayDateSTR]['Bond'].drop_duplicates().dropna().copy()
        new_bonds = list(set(self.traded_bonds)-set(self.displayPositions.index))
        self.th.positions['EODPrice']=self.EODPrices
        self.th.positions['EODPrice'].fillna(0,inplace=True)
        #print new_bonds
        for bond in new_bonds:
            price = 0 #it's a new bond - unlikely we had a price saved yesterday, why even try!
            #price = self.fc.historical_price_query(bonds.loc[bond,'REGS'], yesterday)
            if price==0:
                price = self.th.df[self.th.df['Bond']==bond].iloc[-1]['Price']
            self.th.positions.loc[bond,'EODPrice'] = price
        self.EODPrices = self.th.positions['EODPrice'].copy()
        #Retrieve principal factor for traded bonds
        self.th.positions['PRINCIPAL_FACTOR'] = self.principalFactor
        self.th.positions['RISK_MID'] = self.riskMid
        #Following 2 lines get rid of some runtime warning - possibly a bug - http://stackoverflow.com/questions/30519487/pandas-error-invalid-value-encountered
        self.th.positions['PRINCIPAL_FACTOR'].fillna(0,inplace=True)
        self.th.positions['RISK_MID'].fillna(0,inplace=True)
        ###
        if len(new_bonds)>0:
            dc=dict(zip(new_bonds,map(lambda x:bonds.loc[x,'REGS']+ ' Corp',new_bonds)))
            output=blpapiwrapper.simpleReferenceDataRequest(dc,['PRINCIPAL_FACTOR','RISK_MID'])
            self.th.positions.loc[new_bonds,'PRINCIPAL_FACTOR'] = output['PRINCIPAL_FACTOR'].astype(float)
            self.principalFactor = self.th.positions['PRINCIPAL_FACTOR']
            self.th.positions.loc[new_bonds,'RISK_MID'] = output['RISK_MID'].astype(float)
            self.riskMid = self.th.positions['RISK_MID']
            spbonds = list(set(SPECIALBONDS) & set(new_bonds))
            if len(spbonds)>0:
                dc = dict(zip(spbonds,map(lambda x:bonds.loc[x,'REGS']+ ' Corp',spbonds)))
                output = blpapiwrapper.simpleReferenceDataRequest(dc,['WORKOUT_OAS_MID_MOD_DUR'])
                self.th.positions.loc[spbonds,'RISK_MID'] = output['WORKOUT_OAS_MID_MOD_DUR'].astype(float)
                self.riskMid[spbonds]=self.th.positions.loc[spbonds, 'RISK_MID']

        self.th.positions['USDQty'] = self.th.positions.apply(lambda row:row['Qty']/ccy.loc[row['CCY'],'2016'],axis=1)
        self.th.positions['EODValue'] = self.th.positions['EODPrice']*self.th.positions['USDQty']/100.*(self.th.positions['PRINCIPAL_FACTOR'])
        self.th.positions['Risk'] = -self.th.positions['USDQty']*self.riskMid/10000
        self.th.positions.loc[self.th.positions['Issuer']=='T','Risk'] = 0 # UST HAVE NO CREDIT RISK
        self.th.positions.loc[self.th.positions['Issuer']=='DBR','Risk'] = 0 # BUNDS HAVE NO CREDIT RISK

        self.displayPositions = self.th.positions.loc[list(self.displayPositions.index)+new_bonds]#SOD risk + new trades
        self.displayPositions = self.displayPositions.join(self.cntrymap['Long name'],on='Country')
        self.displayPositions.rename(columns={'Long name':'LongCountry'},inplace=True)
        self.displayGroup = self.displayPositions.groupby(['Region','LongCountry','Issuer','Bond']).sum()
        #print self.displayGroup

        #I'm now downloading prices for existing UST and bund positions
        if self.bdmReady:
            ust_positions_isins = list(self.th.positionsByISINBook.loc[self.th.positionsByISINBook['Issuer']=='T','ISIN'])
            dbr_positions_isins = list(self.th.positionsByISINBook.loc[self.th.positionsByISINBook['Issuer']=='DBR','ISIN'])
            ust_dbr_isins = ust_positions_isins + dbr_positions_isins
            ust_dbr_isins_bbg = map(lambda x:x + '@BGN Corp', ust_dbr_isins)
            dic = dict(zip(ust_dbr_isins, ust_dbr_isins_bbg))
            if len(dic)>0:
                self.usd_dbr_prices = blpapiwrapper.simpleReferenceDataRequest(dic, 'PX_MID')
            else:
                self.usd_dbr_prices = pandas.DataFrame()
            #print self.usd_dbr_prices
            pass

        #Here I'm going to take care of the positionsbyisinbook table
        if self.bdmReady:
            for (i,row) in self.th.positionsByISINBook.iterrows():
                try:
                    self.th.positionsByISINBook.at[i,'PriceT'] = self.bdm.df.at[row['Bond'],'MID']
                except:
                    self.th.positionsByISINBook.at[i,'PriceT'] = pandas.np.nan # for UST and unrecognized bonds
            for (i,row) in self.usd_dbr_prices.iterrows():
                self.th.positionsByISINBook.loc[self.th.positionsByISINBook['ISIN']==i,'PriceT']=float(row['PX_MID'])
                pass


        self.principalFactor.name = 'PRINCIPAL_FACTOR'
        self.riskMid.name = 'RISK_MID'
        self.EODPrices.name = 'EODPrice'
        del self.th.positionsByISINBook['PRINCIPAL_FACTOR']
        del self.th.positionsByISINBook['RISK_MID']
        del self.th.positionsByISINBook['EODPrice']
        self.th.positionsByISINBook = self.th.positionsByISINBook.join(self.EODPrices, on='Bond')
        self.th.positionsByISINBook = self.th.positionsByISINBook.join(self.principalFactor, on='Bond')
        self.th.positionsByISINBook = self.th.positionsByISINBook.join(self.riskMid, on = 'Bond')
        
        self.th.positionsByISINBook['SODPnL'] = self.th.positionsByISINBook['SOD_Pos'] *  self.th.positionsByISINBook['PRINCIPAL_FACTOR'] * (self.th.positionsByISINBook['PriceT'] - self.th.positionsByISINBook['PriceY'])/100.

        self.updateNewTradesByISIN() # at that point prices and principal factors are ready already if self.bdmReady
        
        self.th.positionsByISINBook['SODPnL'].fillna(0, inplace = True) # for bonds with no position at SOD

        self.th.positionsByISINBook['TotalPnL'] = self.th.positionsByISINBook['SODPnL']/self.th.positionsByISINBook.apply(lambda row:ccy.loc[row['CCY'],'2016'],axis=1) + self.th.positionsByISINBook['TradePnL']/self.th.positionsByISINBook.apply(lambda row:ccy.loc[row['CCY'],'2016'],axis=1)

        self.th.positionsByISINBook['USDQty'] = self.th.positionsByISINBook.apply(lambda row:row['Qty']/ccy.loc[row['CCY'],'2016'],axis=1)
        self.th.positionsByISINBook['EODValue'] = self.th.positionsByISINBook['EODPrice']*self.th.positionsByISINBook['USDQty']/100.*(self.th.positionsByISINBook['PRINCIPAL_FACTOR'])
        self.th.positionsByISINBook['Risk'] = -self.th.positionsByISINBook['USDQty']*self.th.positionsByISINBook['RISK_MID']/10000
        self.th.positionsByISINBook.loc[self.th.positionsByISINBook['Issuer']=='T','Risk'] = 0 # UST HAVE NO CREDIT RISK
        self.th.positionsByISINBook.loc[self.th.positionsByISINBook['Issuer']=='DBR','Risk'] = 0 # BUNDS HAVE NO CREDIT RISK

        self.displayPositionsBook=self.th.positionsByISINBook
        self.displayGroupBook = self.th.positionsByISINBook.groupby(['Book','LongCountry','Issuer','Bond','Series']).sum()

        pub.sendMessage('REDRAW_RISK_TREE', message=MessageContainer('empty'))


    def updateNewTradesByISIN(self):
        #THERE SHOULD NOT BE MORE THAN ONE RECORD PER BOOK AND ISIN - THE KEY IS BOOK-ISIN
        self.th.positionsByISINBook = self.th.positionsByISINBook[self.th.positionsByISINBook['SOD_Pos']!=0].copy()
        self.new_trades = self.th.df[self.th.df['Date']==todayDateSTR].copy()
        
        self.new_trades['TradePnL'] = 0
        if self.bdmReady:
            self.new_trades = self.new_trades.join(self.bdm.df['MID'], on='Bond')
            ust_dbr_new_trades_isin = self.new_trades.loc[(self.new_trades['Issuer']=='T') | (self.new_trades['Issuer']=='DBR'),'ISIN']#this works because bond name == isin for UST and bunds but it's not very clean
            #print ust_dbr_new_trades_isin
            if len(ust_dbr_new_trades_isin)>0:
                ust_dbr_new_trades_isin_bbg = map(lambda x:x + '@BGN Corp', ust_dbr_new_trades_isin)
                dic = dict(zip(ust_dbr_new_trades_isin, ust_dbr_new_trades_isin_bbg))
                #print dic
                usd_dbr_new_trades_prices = blpapiwrapper.simpleReferenceDataRequest(dic, 'PX_MID')
                for (i,row) in usd_dbr_new_trades_prices.iterrows():
                    self.new_trades.loc[self.new_trades['ISIN']==i,'MID'] = float(row['PX_MID'])#this works because bond name == isin for UST and bunds but it's not very clean

        self.positionDeltas = self.new_trades.groupby(['Book','ISIN'])[['Qty','MK']]
        reclist = []
        nkeylist = []
        for (k,grp) in self.positionDeltas:
            key=k[0]+'-'+k[1]
            if key in self.th.positionsByISINBook.index:
                self.th.positionsByISINBook.at[key,'Qty'] = self.th.positionsByISINBook.at[key,'SOD_Pos']+grp['Qty'].sum()
                self.th.positionsByISINBook.at[key,'MK'] = grp['MK'].sum()
            else:
                lr=self.new_trades.loc[self.new_trades['ISIN']==k[1]].iloc[-1]#take the last trade -> ONLY FOR STATIC DATA
                bond = lr['Bond']
                #print "New entry for " + bond + ": " + key
                pf = self.th.positions.at[bond,'PRINCIPAL_FACTOR']
                r = self.th.positions.at[bond,'RISK_MID']
                lc = self.cntrymap.at[lr['Country'],'Long name']

                series = 'REGS' if k[1]==bonds.loc[bond,'REGS'] else '144A'
                rec = [bond,k[0],lr['CCY'],k[1],lr['Issuer'], lr['Country'],lc,0,series, grp['Qty'].sum(), grp['MK'].sum(), lr['Price'], pandas.np.nan, pf, r]
                reclist.append(rec)
                nkeylist.append(key)

        if reclist!=[]:
            reclistdf = pandas.DataFrame(data=reclist, columns=['Bond','Book','CCY','ISIN','Issuer','Country','LongCountry','SOD_Pos', 'Series','Qty', 'MK', 'EODPrice', 'PriceY', 'PRINCIPAL_FACTOR','RISK_MID'], index=nkeylist)
            self.th.positionsByISINBook = self.th.positionsByISINBook.append(reclistdf, verify_integrity=True)

        #now I calculate tradepnl
        #print self.th.positionsByISINBook
        if self.bdmReady:
            #print self.th.positionsByISINBook
            #THIS WILL ERROR IF NOT PRICE FEED FOR THE BOND
            for (k,grp) in self.positionDeltas:
                isin=k[1]
                try:
                    bond = allisins[isin]
                except:
                    bond = 'DUMMY'
                idx = (self.new_trades['ISIN']==isin) & (self.new_trades['Book']==k[0])
                #print isin, k[0]
                #print idx
                self.new_trades.loc[idx,'TradePnL']=self.new_trades.loc[idx,'Qty']*(self.new_trades.loc[idx,'MID']-self.new_trades.loc[idx,'Price'])/100.
                try:
                    self.th.positionsByISINBook.at[k[0]+'-'+k[1],'TradePnL'] = self.th.positionsByISINBook.at[k[0]+'-'+k[1],'PRINCIPAL_FACTOR'] * self.new_trades.loc[idx,'TradePnL'].sum()
                except:
                    print 'error finding a price for ' + k[0]+'-'+k[1]
            pass


        #self.th.positionsByISINBook.to_csv(TEMPPATH+'test.csv')

