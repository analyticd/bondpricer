"""
Bond pricer - displays data from Bloomberg and Front, MVC architecture.
Written by Alexandre Almosni   alexandre.almosni@gmail.com
(C) 2014-2015 Alexandre Almosni
Released under Apache 2.0 license. More info at http://www.apache.org/licenses/LICENSE-2.0

Classes:
BondDataModel: the main class, basically a huge table with data getting updated from Bloomberg in real time.
StreamWatcher: helper class to send analytics data (both real time price data and static data) to the BondDataModel
StreamWatcherHistory: helper class to download price history to the BondDataModel

Functions:
getMaturityDate(): helper function to convert Bloomberg date format to datetime format
"""
import wx
from wx.lib.pubsub import pub

import pandas
import blpapiwrapper
import threading
import datetime
import os
import time

from SwapHistory import SwapHistory
from StaticDataImport import ccy, countries, bonds, TEMPPATH, bonduniverseexclusionsList, frontToEmail, SPECIALBONDS, BBGHand, regsToBondName, bbgToBdmDic

class MessageContainer():
    def __init__(self,data):
        self.data = data

def getMaturityDateOld(d):
    """parse maturity date of the bond in MM/DD/YY format. Bloomberg override for perpetual bonds.
    """
    try:
        return datetime.datetime.strptime(d, '%m/%d/%Y')
    except:
        return datetime.datetime(2049, 12, 31)

def getMaturityDate(d):
    """
    Function to parse maturity date in YYYY-MM-DD format
    """
    try:
        output=datetime.datetime.strptime(d,'%Y-%m-%d')
    except:
        output=datetime.datetime(2049,12,31)
    return output


class BondDataModel():
    """BondDataModel class : Class to define the bond data model

    Attributes:
    self.parent = parent frame (Wx.Frame object)
    self.dtToday : datetime.datetime object for today 
    self.dtYesterday : datetime.datetime object for yesterday 
    self.dtLastWeek : datetime.datetime object for last week 
    self.dtLastMonth : datetime.datetime object for last month
    self.mainframe : FlowTradingGUI > MainForm class instance
    self.th : trade history data. (defaults to None if mainframe is not specified. 
                                    This is to allow Pricer to be launched independently without having to connect to Front)
    self.df : pandas.DataFrame consisting of all the bonds' information
    self.USDswapRate : Interpolated US Swap rates
    self.CHFswapRate : Interpolated CHF Swap rates 
    self.EURswapRate : Interpolated EUR Swap rates
    self.CNYswapRate : Interpolated CNY Swap rates 

    Methods:
    __init__()
    reduceUniverse()
    fillPositions()
    updatePrice()
    updateStaticAnalytics()
    updateCell()
    updatePositions()
    startUpdates()
    firstPass()
    reOpenConnection()
    refreshSwapRates()
    fillHistoricalPricesAndRating()
    updateBenchmarks()
    populateRiskFreeRates()
    """
    def __init__(self, parent, mainframe=None):
        """
        Keyword arguments:
        parent : parent frame (Wx.Frame object)
        mainframe : FlowTradingGUI > MainForm class instance (defaults to [] if not specified)
        """
        self.parent = parent
        self.mainframe = mainframe
        self.th = None if mainframe is None else mainframe.th

        self.dtToday = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
        _offsets = (3, 1, 1, 1, 1, 1, 2)
        self.dtYesterday = self.dtToday - datetime.timedelta(days=_offsets[self.dtToday.weekday()])
        self.dtLastWeek = self.dtToday - datetime.timedelta(days=7)
        dtTemp = self.dtToday - datetime.timedelta(days=29)
        self.dtLastMonth = dtTemp - datetime.timedelta(days=_offsets[dtTemp.weekday()])

        # Static columns either in bonduniverse or directly derived from it
        colsDescription = ['ISIN', 'BOND', 'SERIES', 'CRNCY', 'MATURITY', 'COUPON', 'AMT_OUTSTANDING', 'SECURITY_NAME',
                           'INDUSTRY_GROUP', 'CNTRY_OF_RISK', 'TICKER', 'MATURITYDT']
        # these columns will feed from Bloomberg (or Front) and be regenerated only once
        colsPriceHistory = ['P1DFRT', 'P1D', 'P1W', 'P1M', 'Y1D', 'Y1W', 'Y1M','SAVG','SAVG1D','SAVG1W','SAVG1M','ISP1D','ISP1W','ISP1M','INTSWAP1D','INTSWAP1W','INTSWAP1M']
        # these will feed from Bloomberg only once
        colsRating = ['SNP', 'MDY', 'FTC']
        colsAccrued = ['ACCRUED', 'D2CPN']
        # these columns will feed from Bloomberg (or Front) and be regenerated all the time
        colsPrice = ['BID', 'ASK', 'MID', 'BID_SIZE', 'ASK_SIZE']#ADD LAST_UPDATE
        colsAnalytics = ['YLDB', 'YLDA', 'YLDM', 'ZB', 'ZA', 'ZM','INTSWAP','ISP','RSI14','RISK_MID']
        colsChanges = ['DP1FRT', 'DP1D', 'DP1W', 'DP1M', 'DY1D', 'DY1W', 'DY1M','DISP1D','DISP1W','DISP1M']
        colsPosition = ['POSITION']
        self.colsAll = colsDescription + colsPriceHistory + colsRating + colsAccrued + colsPrice + colsAnalytics + colsChanges + colsPosition  # +colsPricingHierarchy+colsUpdate

        self.df = pandas.DataFrame(columns=self.colsAll, index=bonds.index)
        # self.df.drop(bonduniverseexclusionsList,inplace=True)
        self.df['BOND'] = self.df.index
        self.df['ISIN'] = bonds['REGS']
        self.df['SERIES'] = 'REGS'
        self.gridList = []
        self.lock = threading.Lock()
        for c in list(set(colsDescription) & set(bonds.columns)):
            self.df[c] = bonds[c]
        self.df.rename(columns={'AMT_OUTSTANDING': 'SIZE'}, inplace=True)
        #self.df['SIZE'] = self.df['SIZE'].apply(lambda x: '{:,.0f}'.format(float(x) / 1000000) + 'm')
        self.df['MATURITYDT'] = self.df['MATURITY'].apply(getMaturityDate)
        self.df = self.df[self.df['MATURITYDT'] >= self.dtToday]
        self.df['MATURITY'] = self.df['MATURITYDT'].apply(lambda x: x.strftime('%d/%m/%y'))
        self.df['POSITION'] = 0
        pub.subscribe(self.updatePositions, "POSITION_UPDATE")
        self.bondList = []
        self.bbgPriceQuery = ['BID', 'ASK', 'YLD_CNV_BID', 'YLD_CNV_ASK', 'Z_SPRD_BID', 'Z_SPRD_ASK','RSI_14D', 'BID_SIZE', 'ASK_SIZE']
        self.bbgPriceSpecialQuery = ['BID', 'ASK', 'YLD_CNV_BID', 'YLD_CNV_ASK', 'OAS_SPREAD_BID', 'OAS_SPREAD_ASK','RSI_14D', 'BID_SIZE', 'ASK_SIZE']
        pass

    def reduceUniverse(self):
        """Reduce the bond universe to bonds that are in any one grid
        """
        self.bondList = list(set([bond for grid in self.parent.gridList for bond in grid.bondList]))#set removes duplicates
        self.df = self.df.reindex(self.bondList)
        self.df = self.df[pandas.notnull(self.df['ISIN'])]

    def fillPositions(self):
        """Fills positions if trade history data is available
        """
        if self.th is not None:
            self.df['POSITION'] = self.th.positions['Qty']
            self.df['POSITION'].fillna(0, inplace=True)

    def updatePrice(self, isinkey, field, data, bidask):
        """
        Gets called by StreamWatcher. 
        Keyword arguments:
        isinkey : ISIN 
        field : field to update, not used
        data : data, a pandas Series
        bidask : 'BID' fetches new events from bloomberg. 'ANALYTICS', 'FIRSTPASS' or 'RTGACC' updates cells in grid.
        Importantly, there can be a 'BID' event without any data, so one needs to specifically call for the BID as well after an event.
        """
        isin = isinkey[0:12]
        bond = regsToBondName[isin]
        if bidask == 'BID':
            if bond in SPECIALBONDS:
                self.blptsAnalytics.get(isin + BBGHand + ' Corp', self.bbgPriceSpecialQuery)
            else:
                self.blptsAnalytics.get(isin + BBGHand + ' Corp', self.bbgPriceQuery)
        elif bidask == 'RTGACC':
            for item, value in data.iteritems():
                self.updateCell(bond,bbgToBdmDic[item],value)
        else:#'ANALYTICS' or 'FIRSTPASS'
            data = data.astype(float)
            for item, value in data.iteritems():
                self.updateCell(bond,bbgToBdmDic[item],value)
            if bidask == 'ANALYTICS':
                self.updateStaticAnalytics(bond)

    def send_price_update(self, bonddata):
        pub.sendMessage('BOND_PRICE_UPDATE', message=MessageContainer(bonddata))



    def updateStaticAnalytics(self, bond):
        """Updates static analytics.
        """
        self.updateCell(bond, 'MID', (self.df.at[bond, 'BID'] + self.df.at[bond, 'ASK']) / 2.)
        self.updateCell(bond, 'DP1FRT', self.df.at[bond, 'MID'] - self.df.at[bond, 'P1DFRT'])       
        self.updateCell(bond, 'DP1D', self.df.at[bond, 'MID'] - self.df.at[bond, 'P1D'])
        self.updateCell(bond, 'DP1W', self.df.at[bond, 'MID'] - self.df.at[bond, 'P1W'])
        self.updateCell(bond, 'DP1M', self.df.at[bond, 'MID'] - self.df.at[bond, 'P1M'])
        self.updateCell(bond, 'YLDM', (self.df.at[bond, 'YLDB'] + self.df.at[bond, 'YLDA']) / 2.)
        self.updateCell(bond, 'DY1D', (self.df.at[bond, 'YLDM'] - self.df.at[bond, 'Y1D']) * 100)
        self.updateCell(bond, 'DY1W', (self.df.at[bond, 'YLDM'] - self.df.at[bond, 'Y1W']) * 100)
        self.updateCell(bond, 'DY1M', (self.df.at[bond, 'YLDM'] - self.df.at[bond, 'Y1M']) * 100)
        self.updateCell(bond, 'ZM', (self.df.at[bond, 'ZB'] + self.df.at[bond, 'ZA']) / 2.)
        self.updateCell(bond, 'ISP', (self.df.at[bond,'YLDM'] - self.df.at[bond, 'INTSWAP'])*100)
        self.updateCell(bond, 'DISP1D', (self.df.at[bond, 'ISP'] - self.df.at[bond, 'ISP1D']))
        self.updateCell(bond, 'DISP1W', (self.df.at[bond, 'ISP'] - self.df.at[bond, 'ISP1W']))
        self.updateCell(bond, 'DISP1M', (self.df.at[bond, 'ISP'] - self.df.at[bond, 'ISP1M']))
        #self.send_price_update(self.df.loc[bond])
        #print self.df.loc[bond]
        pub.sendMessage('BOND_PRICE_UPDATE', message=MessageContainer(self.df.loc[bond]))

    def updateCell(self, bond, field, value):
        """Thread safe implementation to update individual cells
        This needs to be self.df.loc: self.df.set_value or self.df.at doesn't seem to be updating properly - is it too fast?
        Update 19Jan2016: using Pandas 0.17, .at works. As it's a lot faster (10x) this is what will be used.
        """
        self.lock.acquire()
        self.df.at[bond, field] = value         #self.df.loc[bond, field] = value
        self.lock.release()

    def updatePositions(self, message=None):
        """Updates position
        """
        self.df['POSITION'] = message.data['Qty']
        self.df['POSITION'].fillna(0, inplace=True)

    def startUpdates(self):
        """Starts live feed from Bloomberg.
        """
        self.blptsAnalytics = blpapiwrapper.BLPTS()
        self.streamWatcherAnalytics = StreamWatcher(self, 'ANALYTICS')
        self.blptsAnalytics.register(self.streamWatcherAnalytics)
        self.bbgstreamBID = blpapiwrapper.BLPStream(list((self.df['ISIN'] + BBGHand + ' Corp').astype(str)), 'BID', 0)
        self.streamWatcherBID = StreamWatcher(self,'BID')
        self.bbgstreamBID.register(self.streamWatcherBID)
        self.bbgstreamBID.start()
        pass

    def firstPass(self, priorityBondList=[]):
        """Loads initial data upon start up. After downloading data on first pass, function will check for bonds
        in SPECIALBONDS and will overwrite downloaded data with new set of data. 

        Keyword arguments:
        priortyBondList : priorty bonds to be updated (defaults to [] if not specified)
        """

        self.USDswapRate = SwapHistory('USD',self.dtToday)
        self.CHFswapRate = SwapHistory('CHF',self.dtToday)
        self.EURswapRate = SwapHistory('EUR',self.dtToday)
        self.CNYswapRate = SwapHistory('CNY',self.dtToday)
        currencyList = {'USD':self.USDswapRate,'CHF':self.CHFswapRate,'EUR':self.EURswapRate,'CNY':self.CNYswapRate}
        self.populateRiskFreeRates(currencyList,'INTSWAP','SAVG')

        emptyLines = list(self.df.index) if priorityBondList == [] else priorityBondList
        isins = self.df.loc[emptyLines, 'ISIN'] + BBGHand + ' Corp'
        isins = list(isins.astype(str))
        blpts = blpapiwrapper.BLPTS(isins, self.bbgPriceQuery)
        blptsStream = StreamWatcher(self,'FIRSTPASS')
        blpts.register(blptsStream)
        blpts.get()
        blpts.closeSession()

        specialBondList = list(set(emptyLines) & set(SPECIALBONDS))
        specialIsins = map(lambda x:self.df.at[x,'ISIN'] + BBGHand + ' Corp',specialBondList)
        blpts = blpapiwrapper.BLPTS(specialIsins, self.bbgPriceSpecialQuery)
        specialbondStream = StreamWatcher(self,'FIRSTPASS')
        blpts.register(specialbondStream)
        blpts.get()
        blpts.closeSession()

        for bond in emptyLines:
            self.updateStaticAnalytics(bond)  # This will update benchmarks and fill grid. Has to be done here so all data for benchmarks is ready.

    def reOpenConnection(self):
        """Reopens bloomberg connection. Function is called when the 'Restart Bloomberg Connection' button from the pricer frame is clicked
        """
        self.blptsAnalytics.closeSession()
        self.blptsAnalytics = None
        self.bbgstreamBID.closeSubscription()
        self.bbgstreamBID = None
        self.streamWatcherBID = None
        self.streamWatcherAnalytics = None
        self.firstPass()
        self.startUpdates()

    def refreshSwapRates(self):
        """Refreshes the swap rates. Function is called when the 'Refresh Rates' button from the pricer menu is clicked.
        """
        self.USDswapRate.refreshRates()
        self.CHFswapRate.refreshRates()
        self.EURswapRate.refreshRates()
        self.CNYswapRate.refreshRates()
        currencyList = {'USD':self.USDswapRate,'CHF':self.CHFswapRate,'EUR':self.EURswapRate,'CNY':self.CNYswapRate}
        self.populateRiskFreeRates(currencyList,'INTSWAP','SAVG')
        for bond in list(self.df.index):
            self.updateStaticAnalytics(bond)

    def fillHistoricalPricesAndRating(self):
        """Fill historical prices and ratings. Function is called when the pricer menu first launches. 
        """
        time_start = time.time()
        savepath = TEMPPATH+'bondhistoryrating.csv'
        #If bondhistoryratingUAT.csv doesn't exist, download data and write file.
        if not (os.path.exists(savepath)) or datetime.datetime.fromtimestamp(
                os.path.getmtime(savepath)).date() < datetime.datetime.today().date():
            isins = self.df['ISIN'] + BBGHand + ' Corp'
            isins = list(isins.astype(str))

            rtgaccBLP = blpapiwrapper.BLPTS(isins,
                                            ['RTG_SP', 'RTG_MOODY', 'RTG_FITCH', 'INT_ACC', 'DAYS_TO_NEXT_COUPON','YRS_TO_SHORTEST_AVG_LIFE','RISK_MID','PRINCIPAL_FACTOR','AMT_OUTSTANDING'])
            rtgaccStream = StreamWatcher(self,'RTGACC')
            rtgaccBLP.register(rtgaccStream)
            rtgaccBLP.get()
            rtgaccBLP.closeSession()

            priceHistory = blpapiwrapper.BLPTS(isins, ['PX_LAST', 'YLD_YTM_MID'], startDate=self.dtLastMonth,endDate=self.dtToday)
            priceHistoryStream = StreamWatcherHistory(self)
            priceHistory.register(priceHistoryStream)
            priceHistory.get()
            priceHistory.closeSession()

            #Based on today's shortest to average life, calculate the SAVG for yesterday, last week, and last month
            self.df['SAVG'] = self.df['SAVG'].astype(float)
            self.df['SAVG1D'] = self.df['SAVG']+(self.dtToday - self.dtYesterday).days/365.0
            self.df['SAVG1W'] = self.df['SAVG']+(self.dtToday - self.dtLastWeek).days/365.0
            self.df['SAVG1M'] = self.df['SAVG']+(self.dtToday - self.dtLastMonth).days/365.0

            #Create DataFrames for Swap Rates of different currencies
            US1D = SwapHistory('USD',self.dtYesterday)
            US1W = SwapHistory('USD',self.dtLastWeek)
            US1M = SwapHistory('USD',self.dtLastMonth)
            CHF1D = SwapHistory('CHF',self.dtYesterday)
            CHF1W = SwapHistory('CHF',self.dtLastWeek)
            CHF1M = SwapHistory('CHF',self.dtLastMonth)
            EUR1D = SwapHistory('EUR',self.dtYesterday)
            EUR1W = SwapHistory('EUR',self.dtLastWeek)
            EUR1M = SwapHistory('EUR',self.dtLastMonth)
            CNY1D = SwapHistory('CNY',self.dtYesterday)
            CNY1W = SwapHistory('CNY',self.dtLastWeek)
            CNY1M = SwapHistory('CNY',self.dtLastMonth)         
            #Compute Historical Risk Free Rate for each bonds.
            currencyList1D = {'USD':US1D,'CHF':CHF1D,'EUR':EUR1D,'CNY':CNY1D}
            self.populateRiskFreeRates(currencyList1D,'INTSWAP1D','SAVG1D')
            currencyList1W = {'USD':US1W,'CHF':CHF1W,'EUR':EUR1W,'CNY':CNY1W}
            self.populateRiskFreeRates(currencyList1W,'INTSWAP1W','SAVG1W')           
            currencyList1M = {'USD':US1M,'CHF':CHF1M,'EUR':EUR1M,'CNY':CNY1M}
            self.populateRiskFreeRates(currencyList1M,'INTSWAP1M','SAVG1M')
            # get ISpread over past dates
            self.df['ISP1D'] = (self.df['Y1D']-self.df['INTSWAP1D'])*100
            self.df['ISP1W'] = (self.df['Y1W']-self.df['INTSWAP1W'])*100
            self.df['ISP1M'] = (self.df['Y1M']-self.df['INTSWAP1M'])*100

            self.df[['SNP', 'MDY', 'FTC', 'P1D', 'P1W', 'P1M', 'Y1D', 'Y1W', 'Y1M', 'ACCRUED', 'D2CPN','SAVG','ISP1D','ISP1W','ISP1M','RISK_MID','PRINCIPAL_FACTOR','SIZE']].to_csv(savepath)
            self.df['ACCRUED'] = self.df['ACCRUED'].apply(lambda x: '{:,.2f}'.format(float(x)))
            self.df['D2CPN'].fillna(-1, inplace=True)
            self.df['D2CPN'] = self.df['D2CPN'].astype(int)
            self.df[['SNP', 'MDY', 'FTC']] = self.df[['SNP', 'MDY', 'FTC']].fillna('NA')  # ,'ACCRUED','D2CPN'
            self.df[['SNP', 'MDY', 'FTC', 'ACCRUED']] = self.df[['SNP', 'MDY', 'FTC', 'ACCRUED']].astype(str)

        #Otherwise, load and read from file.
        else:
            print 'Found existing file from today'
            df = pandas.read_csv(savepath, index_col=0)
            self.df[['SNP', 'MDY', 'FTC', 'P1D', 'P1W', 'P1M', 'Y1D', 'Y1W', 'Y1M', 'ACCRUED', 'D2CPN','SAVG','ISP1D','ISP1W','ISP1M']] = df[['SNP', 'MDY', 'FTC', 'P1D', 'P1W', 'P1M', 'Y1D', 'Y1W', 'Y1M', 'ACCRUED', 'D2CPN','SAVG','ISP1D','ISP1W','ISP1M']]
            try:
                self.df[['RISK_MID','PRINCIPAL_FACTOR','SIZE']] = df[['RISK_MID','PRINCIPAL_FACTOR','SIZE']]
            except:
                pass
            self.df[['SNP', 'MDY', 'FTC']] = self.df[['SNP', 'MDY', 'FTC']].astype(str)
            self.df['ACCRUED'].fillna(-1,inplace=True)#HACK SO NEXT LINE DOESN'T BLOW UP - WE DON'T WANT TO PUT 0 THERE!
            self.df['ACCRUED'] = self.df['ACCRUED'].astype(float)
            self.df['ACCRUED'] = self.df['ACCRUED'].apply(lambda x: '{:,.2f}'.format(float(x)))
            self.df['D2CPN'].fillna(-1, inplace=True)#HACK SO NEXT LINE DOESN'T BLOW UP - WE DON'T WANT TO PUT 0 THERE!
            self.df['D2CPN'] = self.df['D2CPN'].astype(int)          
            self.df[['SAVG','ISP1D','ISP1W','ISP1M']] = self.df[['SAVG','ISP1D','ISP1W','ISP1M']].astype(float)

        pxhist = self.df[['P1D', 'P1W', 'P1M', 'Y1D', 'Y1W', 'Y1M']]
        #print pxhist[pxhist.isnull().any(axis=1)]
        print 'History fetched in: ' + str(int(time.time() - time_start)) + ' seconds.'

    def updateBenchmarks(self):
        """Updates benchmarks
        """
        for grid in self.gridList:
            grid.updateBenchmarks()

    def populateRiskFreeRates(self,curncyDic,swapCol,savgCol):
        """Populates the risk free rates. Risk free rate is calculating the interpolated swap rate on the bond's shortest to average life.
        Keyword argument:
        curncyDic : dictionary of currencies
        swapCol : relevant swap rate. E.g. to populate risk free rate for last week, SwapCol = 'INTSWAP1W'  
        savgCol : relevant shortest average life. E.g. to populate SAVG for yesterday savgCol = 'SAVG1D'
        """
        for currency in curncyDic:
            self.df.loc[bonds['CRNCY']==currency,swapCol] = self.df.loc[bonds['CRNCY']==currency][savgCol].apply(curncyDic[currency].getRateFromYears)


class StreamWatcher(blpapiwrapper.Observer):
    """StreamWatcher class : Class to stream and update analytic data from Bloomberg
    BID keyword for watching events, ANALYTICS to get everything once event triggered, FIRSTPASS for first pass, RTGACC for ratings
    """
    def __init__(self, bdm, bidask='BID'):
        self.bdm = bdm
        self.bidask = bidask

    def update(self, *args, **kwargs):
        if kwargs['field'] == 'ALL':
            self.bdm.updatePrice(kwargs['security'], kwargs['field'], kwargs['data'], self.bidask)


class StreamWatcherHistory(blpapiwrapper.Observer):
    """StreamWatcherHistory class : Class to download historical price and yield data from Bloomberg. Function is called
    by fillHistoricalPricesAndRating on first pass.
    """
    def __init__(self, bdm):
        self.bdm = bdm

    def update(self, *args, **kwargs):
        if kwargs['field'] == 'ALL':
            isin = kwargs['security'][0:12]
            bond = regsToBondName[isin]

            if self.bdm.dtYesterday in kwargs['data'].index:
                self.bdm.updateCell(bond, 'P1D', kwargs['data'].at[self.bdm.dtYesterday, 'PX_LAST'])
                self.bdm.updateCell(bond, 'Y1D', kwargs['data'].at[self.bdm.dtYesterday, 'YLD_YTM_MID'])
            else:
                self.bdm.updateCell(bond, 'P1D', pandas.np.nan)
                self.bdm.updateCell(bond, 'Y1D', pandas.np.nan)

            if self.bdm.dtLastWeek in kwargs['data'].index:
                self.bdm.updateCell(bond, 'P1W', kwargs['data'].at[self.bdm.dtLastWeek, 'PX_LAST'])
                self.bdm.updateCell(bond, 'Y1W', kwargs['data'].at[self.bdm.dtLastWeek, 'YLD_YTM_MID'])
            else:
                self.bdm.updateCell(bond, 'P1W', pandas.np.nan)
                self.bdm.updateCell(bond, 'Y1W', pandas.np.nan)

            if self.bdm.dtLastMonth in kwargs['data'].index:
                self.bdm.updateCell(bond, 'P1M', kwargs['data'].at[self.bdm.dtLastMonth, 'PX_LAST'])
                self.bdm.updateCell(bond, 'Y1M', kwargs['data'].at[self.bdm.dtLastMonth, 'YLD_YTM_MID'])
            else:
                self.bdm.updateCell(bond, 'P1M', pandas.np.nan)
                self.bdm.updateCell(bond, 'Y1M', pandas.np.nan)

