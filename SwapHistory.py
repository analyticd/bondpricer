"""
Bond relative value charting tools and maintenance tools
Written by Sheng Chai and Alexandre Almosni
(C) 2015-2016 Sheng Chai and Alexandre Almosni
Released under Apache 2.0 license. More info at http://www.apache.org/licenses/LICENSE-2.0


Steps to add currencies:

1) SwapHistory.py > Create a dictionary of swap tickers for that currency.
2) BondDataModel.py > fillHistoricalPricesAndRating() > Create DataFrames for Swap Rates of different currencies
3) BondDataModel.py > fillHistoricalPricesAndRating() > Add newly created DataFrames to currencyList dictionaries for all time preriods (today, 1D, 1W, 1M)
4) BondDataModel.py > refreshSwapRates() > Add currency to function
5) REMEMBER TO DELETE THE bondhistory.csv FILE BEFORE YOU TEST!
"""
import matplotlib.pyplot as plt
import pandas
import blpapiwrapper
import datetime
import scipy.interpolate


#Defining currency swaps and maturity (in years)
USDtickers={'USSW1 curncy': 1, 'USSW2 curncy': 2, 'USSW3 curncy': 3, 'USSW4 curncy': 4, 'USSW5 curncy': 5, 'USSW6 curncy': 6, 'USSW7 curncy': 7, 'USSW8 curncy': 8, 'USSW9 curncy': 9, 'USSW10 curncy': 10, 'USSW15 curncy': 15, 'USSW20 curncy': 20, 'USSW25 curncy': 25, 'USSW30 curncy': 30}
EURtickers={'EUSA1 curncy': 1, 'EUSA2 curncy': 2, 'EUSA3 curncy': 3, 'EUSA4 curncy': 4, 'EUSA5 curncy': 5, 'EUSA6 curncy': 6, 'EUSA7 curncy': 7, 'EUSA8 curncy': 8, 'EUSA9 curncy': 9, 'EUSA10 curncy': 10, 'EUSA15 curncy': 15, 'EUSA20 curncy': 20, 'EUSA25 curncy': 25, 'EUSA30 curncy': 30}
CHFtickers={'SFSW1 curncy': 1, 'SFSW2 curncy': 2, 'SFSW3 curncy': 3, 'SFSW4 curncy': 4, 'SFSW5 curncy': 5, 'SFSW6 curncy': 6, 'SFSW7 curncy': 7, 'SFSW8 curncy': 8, 'SFSW9 curncy': 9, 'SFSW10 curncy': 10, 'SFSW15 curncy': 15, 'SFSW20 curncy': 20, 'SFSW25 curncy': 25, 'SFSW30 curncy': 30}
CNYtickers={'CCSWN1 curncy': 1, 'CCSWN2 curncy': 2, 'CCSWN3 curncy': 3, 'CCSWN4 curncy': 4, 'CCSWN5 curncy': 5, 'CCSWN7 curncy': 7, 'CCSWN10 curncy': 10}
allSwapTickers={'USD':USDtickers,'EUR':EURtickers,'CHF':CHFtickers,'CNY':CNYtickers}

#Defining Libor Tickers and Maturity (in years)
USLiborTickers={'US00O/N Index': 0.002740, 'US0003M Index': 0.25, 'US0006M Index': 0.5, }
EURLiborTickers={'EUDR1T curncy': 0.002740, 'EUR003M Index': 0.25, 'EUR006M Index': 0.5, }
CHFLiborTickers={'SFDR1T curncy': 0.002740, 'SF0003M Index': 0.25, 'SF0006M Index': 0.5, }
CNYLiborTickers={'SHIFON Index': 0.002740, 'SHIF3M Index': 0.25, 'SHIF6M Index': 0.5, }
allLiborTickers={'USD':USLiborTickers,'EUR':EURLiborTickers,'CHF':CHFLiborTickers,'CNY':CNYLiborTickers}

class HistoryRequest(blpapiwrapper.Observer):
    def __init__(self,bondisins):
        self.bondisinsDC={}
    def update(self, *args, **kwargs):
        if kwargs['field']!='ALL':
            self.bondisinsDC[kwargs['security']]=kwargs['data']

class SwapHistory():
    '''
    Class to construct yield curve.

    Attributes:
        self.anchorDate: date of query(defines the date of the overnight interbank rate)
        self.curncy: curreny of the swaps
        self.swapTickers: ticker symbols of the currency swaps
        self.self.lastRefreshTime: last refresh time 
        self.interpolatedFunction : interpolated swap rates 

    Methods:
        refreshRates() : Download the swap rates from Bloombergs
        getRateFromDate() : query the swap rate using "Dates" (datetime.datetime object)
        getRateFromYears() : query the swap rate using "Number of Years" (datetime.datetime object) 
        plot() : plot swap rates against tenor
    '''
    def __init__(self,curncy,anchorDate):
        """
        Keyword arguments:
        curncy : curreny of bond 
        self.anchorDate : anchorDate 
        """
        self.anchorDate = anchorDate
        self.curncy = curncy
        assert self.curncy in allSwapTickers.keys()
        self.swapTickers=allSwapTickers[self.curncy]
        self.LiborTickers=allLiborTickers[self.curncy]
        #self.lastRefreshTime=0
        self.refreshRates()


        pass

    def refreshRates(self):
        '''
        Download swap rates from Bloomberg. If query date = datetime.datetime.today.date(), pulls latest price from Bloomberg.
        If query date !=datetime.datetime.date(), pulls data on the specified query date, e.g. yesterday, last week, last month

        Also creates an interpolated function using scipy.interpolate.interp1d and creates a lastRefreshTime attribute 
        to record the time when the rates was last donwloaded from Bloomberg

        Note:
            Swap rates and Libor rates are downloaded separately:

            1) Function first download swap rates and check to make sure that all swap rate data is available.
               If they are not, function will download the swap rates for the previous day. Step 1 happens 
               recursively until all swap data is present and valid.

            2) Function will download Libor rates for the same day as the swap rates. If Libor rates are unavailable
               because of bank holidays etc, rates for the previous day will be downloaded instead.

            3) Swap rates and Libor rates are therefore not necessarily on the same day. 




        '''
        if self.anchorDate.date()==datetime.datetime.today().date():

            request = blpapiwrapper.BLPTS(self.swapTickers.keys(),'LAST_PRICE')
            request.get()
            self.df=request.output.copy()
            request.closeSession()
            request = None
            request = blpapiwrapper.BLPTS(self.LiborTickers.keys(),'LAST_PRICE')
            request.get()
            self.df2=request.output.copy()
            request.closeSession()
            request = None

        else:
            #Download Swap Rates

            request = blpapiwrapper.BLPTS(self.swapTickers.keys(),'LAST_PRICE',startDate=self.anchorDate,endDate=self.anchorDate)
            hr = HistoryRequest(self.swapTickers.keys())
            request.register(hr)
            request.get()
            request.closeSession()
            request = None
            self.df = pandas.DataFrame(index=hr.bondisinsDC.keys(),columns=['LAST_PRICE'])
            #Download Libor rates

            #Populate Swap Rate DF. 
            for i in hr.bondisinsDC:
                try:
                    #Try to convert them to float for each swap data downlaoded.
                    self.df.loc[i] = float(hr.bondisinsDC[i].values)
                except:
                    #If converting to float fails, we leave the dataframe empty.
                    pass

            #Let notavail = number of data in df where its entries is missing => Rates for these swaps are not downloaded
            #Because they are unvailable! 
            swapNotavail = len(self.df[self.df.isnull().any(axis=1)])

            #While notavail is not 0, we will minus anchorDate by 1 day and try downlaoding the data again.
            #This is because of bank holidays/ other events resulting in swap rates being unavailable for that day.
            #For those days, we'll minus anchorDate by 1 and download the data again.
            newDate = self.anchorDate

            #While loop to check for Swap Rates
            while swapNotavail !=0:
                #Print out the anchorDates to trace the dates.
                print 'Swap rates for ',self.curncy,' on ',newDate,' is unavailable. Downloading ',self.curncy,' rates on ',newDate - datetime.timedelta(days=1)
                newDate = newDate - datetime.timedelta(days=1)
                
                #Limit backtracking of dates to 10. If more than 10 days, there might be problem with the swaptickers!
                #Suggest that you check the swaptickers to make sure that its valid. 
                #For example, I encountered this problem when adding chinese swaps. Data for hinese swaps of tenor > 10 years aren't
                #Available so I removed them.

                if newDate == self.anchorDate - datetime.timedelta(days=10):
                    print ('Date Error! Check SwapTicker inputs!')
                    break
                
                else:
                    #Re-downlaod the dates with the new anchorDate
                    request = blpapiwrapper.BLPTS(self.swapTickers.keys(),'LAST_PRICE',startDate=newDate,endDate=newDate)
                    hr = HistoryRequest(self.swapTickers.keys())
                    request.register(hr)
                    request.get()
                    request.closeSession()
                    request = None

                    #Repopulate the df.
                    self.df = pandas.DataFrame(index=hr.bondisinsDC.keys(),columns=['LAST_PRICE'])
                    for i in hr.bondisinsDC:
                        try :
                            #print hr.bondisinsDC[i].values
                            self.df.loc[i] = float(hr.bondisinsDC[i].values)
                        except:
                            pass
                    #Update notavail!
                    swapNotavail = len(self.df[self.df.isnull().any(axis=1)])
            
            #Download Libor Rates 
            liborDate = newDate

            request = blpapiwrapper.BLPTS(self.LiborTickers.keys(),'LAST_PRICE',startDate=liborDate,endDate=liborDate)
            hr2 = HistoryRequest(self.LiborTickers.keys())
            request.register(hr2)
            request.get()
            request.closeSession()
            request = None
            self.df2 = pandas.DataFrame(index=hr2.bondisinsDC.keys(),columns=['LAST_PRICE'])
            
            #Populate Libor Rate DF
            for i in hr2.bondisinsDC:
                try:
                    self.df2.loc[i] = float(hr2.bondisinsDC[i].values)
                except:
                    pass

            liborNotavail = len(self.df2[self.df2.isnull().any(axis=1)])   

            #While loop to check for Libor Rates
            while liborNotavail !=0:
                #Print out the anchorDates to trace the dates.
                print 'Libor rates for ',self.curncy,' on ',liborDate,' is unavailable. Downloading ',self.curncy,' rates on ', liborDate - datetime.timedelta(days=1)
                liborDate = liborDate - datetime.timedelta(days=1)
                
                #Limit backtracking of dates to 10. If more than 10 days, there might be problem with the swaptickers!
                #Suggest that you check the swaptickers to make sure that its valid. 
                #For example, I encountered this problem when adding chinese swaps. Data for hinese swaps of tenor > 10 years aren't
                #Available so I removed them.

                if liborDate == self.anchorDate - datetime.timedelta(days=10):
                    print ('Date Error! Check LiborTicker inputs!')
                    break
                
                else:
                    #Re-downlaod the dates with the new anchorDate
                    request = blpapiwrapper.BLPTS(self.LiborTickers.keys(),'LAST_PRICE',startDate=liborDate,endDate=liborDate)
                    hr2 = HistoryRequest(self.LiborTickers.keys())
                    request.register(hr2)
                    request.get()
                    request.closeSession()
                    request = None

                    #Repopulate the df.
                    self.df2 = pandas.DataFrame(index=hr2.bondisinsDC.keys(),columns=['LAST_PRICE'])
                    for i in hr2.bondisinsDC:
                        try :
                            #print hr.bondisinsDC[i].values
                            self.df2.loc[i] = float(hr2.bondisinsDC[i].values)
                        except:
                            pass
                    #Update notavail!
                    liborNotavail = len(self.df2[self.df2.isnull().any(axis=1)])
   
        self.df['years'] = pandas.Series(index=self.swapTickers.keys(),data=self.swapTickers.values())
        self.df2['years'] = pandas.Series(index=self.LiborTickers.keys(),data=self.LiborTickers.values())
        self.df = self.df.append(self.df2)


        self.df.sort_values(by='years',inplace=True)
        self.df=self.df.astype(float)
        self.interpolationFunction = scipy.interpolate.InterpolatedUnivariateSpline(self.df['years'],self.df['LAST_PRICE'],k=3)
        self.lastRefreshTime=datetime.datetime.now()
        pass




    def getRateFromDate(self,inputDatetime):
        '''
        Queries the interpolated rates using date. Function gets get the number of days (in years) between queried date and 
        anchor date and calls getRateFromYears to return a swap rate (float)
        '''
        return self.getRateFromYears((inputDatetime-self.anchorDate).days/365)

    def getRateFromYears(self,inputYears):
        '''
        Queries the interpolated rates using number of years. Returns a float if input is valid number, and returns 
        a numpy.nan object if input is not valid. 
        ''',
        try:
            x = float(self.interpolationFunction(inputYears))
        except:
            x=pandas.np.nan
        return x

    def plot(self):
        '''
        Plots the interpolated swap rates against the swaps' tenors (overnight to 30 years)
        '''
        #plot interpolated swap rate for sanity check
        xRange=pandas.np.arange(0.00274,30,0.0001)
        yRange=self.interpolationFunction(xRange)
        dateStamp=datetime.datetime.today().strftime('%d-%m-%Y %H:%M:%S')
        plt.title('Swap Rates on %s'%(dateStamp))
        plt.ylabel('Last Price')
        plt.xlabel('Tenor')
        plt.plot(xRange,yRange)
        plt.plot(self.df['years'],self.df['LAST_PRICE'],'ro')
        plt.show()



