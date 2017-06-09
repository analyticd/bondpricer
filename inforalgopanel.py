"""
Inforalgo control panel
Written by Alexandre Almosni   alexandre.almosni@gmail.com
(C) 2016 Alexandre Almosni
Released under Apache 2.0 license. More info at http://www.apache.org/licenses/LICENSE-2.0

"""

import wx
#import datetime
import wx.grid as gridlib
import inforalgo
from wx.lib.scrolledpanel import ScrolledPanel

# def wxdate2pydate(date):
#     """Function to convert wx.datetime to datetime.datetime format
#     """
#     assert isinstance(date, wx.DateTime)
#     if date.IsValid():
#         ymd = map(int, date.FormatISODate().split('-'))
#         return datetime.datetime(*ymd)
#     else:
#         return None

class InforalgoControlPanel(ScrolledPanel):
    def __init__(self, parent, uat_table = None, prd_table = None, bdm = None):
        ScrolledPanel.__init__(self, parent)
        self.SetupScrolling(scroll_x=False, scroll_y=True)
        self.parent = parent
        self.uat_table = uat_table
        self.prd_table = prd_table
        self.bdm = bdm
        self.topSizer = wx.BoxSizer(wx.VERTICAL)
        #ADD ONE RECORD
        self.boxAddRecord = wx.StaticBox(self,label = 'Add record to Inforalgo table')
        self.sizerAddRecord = wx.StaticBoxSizer(self.boxAddRecord,wx.HORIZONTAL)   
        self.isinAddButton = wx.Button(self, label = "Add!")
        self.isinAddButton.Bind(wx.EVT_BUTTON,self.onIsinAddButton)
        self.inputGrid = gridlib.Grid(self)
        self.inputGrid.CreateGrid(1,5)
        self.inputGrid.EnableEditing = True
        self.inputGrid.SetColLabelValue(0,'ISIN')
        self.inputGrid.SetColLabelValue(1,'Bid')
        self.inputGrid.SetColLabelValue(2,'Ask')
        self.inputGrid.SetColLabelValue(3,'Bid size (M)')
        self.inputGrid.SetColLabelValue(4,'Ask size (M)')
        self.sizerAddRecord.Add(self.inputGrid,proportion=0,flag=wx.ALL,border=5)
        self.sizerAddRecord.Add(self.isinAddButton,proportion=0,flag=wx.ALL,border=5)
        #DELETE ONE RECORD
        self.boxDeleteRecord = wx.StaticBox(self,label = 'Delete record from Inforalgo table')
        self.sizerDeleteRecord = wx.StaticBoxSizer(self.boxDeleteRecord,wx.HORIZONTAL)
        self.isinDeleteCtrl = wx.TextCtrl(self, -1, 'ISIN')
        self.isinDeleteButton = wx.Button(self, label = "Delete!")
        self.isinDeleteButton.Bind(wx.EVT_BUTTON, self.onIsinDeleteButton)
        self.sizerDeleteRecord.Add(self.isinDeleteCtrl,proportion=0,flag=wx.ALL,border=5)
        self.sizerDeleteRecord.Add(self.isinDeleteButton,proportion=0,flag=wx.ALL,border=5)
        #ADD PRICER RECORDS
        self.boxAddPricerRecords = wx.StaticBox(self,label = 'Add Pricer records to Inforalgo table')
        self.sizerAddPricerRecords = wx.StaticBoxSizer(self.boxAddPricerRecords,wx.HORIZONTAL)
        txtAddPricerWarning = wx.StaticText(self, label="Adds all ISINs in the current Pricer window.")
        self.addPricerRecordsButton = wx.Button(self, label = "Add!")
        self.addPricerRecordsButton.Bind(wx.EVT_BUTTON,self.onAddPricerRecordsButton)
        self.sizerAddPricerRecords.Add(txtAddPricerWarning,proportion=0,flag=wx.ALL,border=5)
        self.sizerAddPricerRecords.Add(self.addPricerRecordsButton,proportion=0,flag=wx.ALL,border=5)
        #DELETE PRICER RECORDS
        self.boxDeletePricerRecords = wx.StaticBox(self,label = 'Delete Pricer records from Inforalgo table')
        self.sizerDeletePricerRecords = wx.StaticBoxSizer(self.boxDeletePricerRecords,wx.HORIZONTAL)
        txtDeletePricerWarning = wx.StaticText(self, label="Deletes all ISINs in the current Pricer window.")
        self.deletePricerRecordsButton = wx.Button(self, label = "Delete!")
        self.deletePricerRecordsButton.Bind(wx.EVT_BUTTON,self.onDeletePricerRecordsButton)
        self.sizerDeletePricerRecords.Add(txtDeletePricerWarning,proportion=0,flag=wx.ALL,border=5)
        self.sizerDeletePricerRecords.Add(self.deletePricerRecordsButton,proportion=0,flag=wx.ALL,border=5)
        #UPDATE TIMESTAMPS FOR PRICER RECORDS
        self.boxUpdateTimeStampsRecords = wx.StaticBox(self,label = 'Update timestamps for all Pricer records')
        self.sizerUpdateTimeStampsRecords = wx.StaticBoxSizer(self.boxUpdateTimeStampsRecords,wx.HORIZONTAL)
        txtUpdateTimeStampsRecordsWarning = wx.StaticText(self, label="Sets the last update time to now.")
        self.updateTimeStampsRecordsButton = wx.Button(self, label = "Update!")
        self.updateTimeStampsRecordsButton.Bind(wx.EVT_BUTTON,self.onUpdateTimeStampsRecordsButton)
        self.sizerUpdateTimeStampsRecords.Add(txtUpdateTimeStampsRecordsWarning,proportion=0,flag=wx.ALL,border=5)
        self.sizerUpdateTimeStampsRecords.Add(self.updateTimeStampsRecordsButton,proportion=0,flag=wx.ALL,border=5)
        #UPDATE PRICE AND SIZES FROM INFORALGO TABLE
        self.boxUpdateFromTable = wx.StaticBox(self,label = 'Update price and sizes from Inforalgo table')
        self.sizerUpdateFromTable = wx.StaticBoxSizer(self.boxUpdateFromTable,wx.HORIZONTAL)
        txtUpdateFromTable = wx.StaticText(self, label="Pushes data if it's in Inforalgo table AND Pricer.")
        self.updateFromTableButton = wx.Button(self, label = "Update!")
        self.updateFromTableButton.Bind(wx.EVT_BUTTON,self.onUpdateFromTableButton)
        self.sizerUpdateFromTable.Add(txtUpdateFromTable,proportion=0,flag=wx.ALL,border=5)
        self.sizerUpdateFromTable.Add(self.updateFromTableButton,proportion=0,flag=wx.ALL,border=5)
        #DELETE ALL RECORDS
        self.boxDeleteAllRecords = wx.StaticBox(self,label = 'Delete all Records from Inforalgo table')
        self.sizerDeleteAllRecords = wx.StaticBoxSizer(self.boxDeleteAllRecords,wx.HORIZONTAL)
        txtDeleteAllPricerWarning = wx.StaticText(self, label="Warning: this will clear the database and affect other users. The password is in the source file.")
        self.isinDeleteAllCtrl = wx.TextCtrl(self, -1, 'Password')
        self.isinDeleteAllButton = wx.Button(self, label = "Delete all!")
        self.isinDeleteAllButton.Bind(wx.EVT_BUTTON, self.onDeleteAllRecordsButton)
        self.sizerDeleteAllRecords.Add(txtDeleteAllPricerWarning,proportion=0,flag=wx.ALL,border=5)
        self.sizerDeleteAllRecords.Add(self.isinDeleteAllCtrl,proportion=0,flag=wx.ALL,border=5)
        self.sizerDeleteAllRecords.Add(self.isinDeleteAllButton,proportion=0,flag=wx.ALL,border=5)
        #INFORALGO PRD TABLE DISPLAY
        self.boxPRDTableDisplay = wx.StaticBox(self,label = 'Inforalgo PRD table contents')
        self.sizerPRDTableDisplay = wx.StaticBoxSizer(self.boxPRDTableDisplay,wx.VERTICAL)   
        self.refreshButtonPRD = wx.Button(self, label = "Refresh!")
        self.refreshButtonPRD.Bind(wx.EVT_BUTTON, self.onRefreshButtonPRD)
        self.inforalgoGridPRD = gridlib.Grid(self)
        self.inforalgoGridPRD.ShowScrollbars(wx.SHOW_SB_NEVER,wx.SHOW_SB_ALWAYS)
        self.inforalgoGridRowsPRD = 10
        self.inforalgoGridPRD.CreateGrid(self.inforalgoGridRowsPRD,9)#one extra empty col at the end
        self.inforalgoGridPRD.SetColSize(8,20)#that extra col is small, the scrollbar will be on top
        self.inforalgoGridPRD.EnableEditing = False
        inforalgoGridCols = ['bbrgDate','bbrgTime','bbrgStatus','bbrgSec6id','bbrgVala','bbrgValc','bbrgValb','bbrgVald']
        for (i,h) in enumerate(inforalgoGridCols):
            self.inforalgoGridPRD.SetColLabelValue(i,h)
        self.sizerPRDTableDisplay.Add(self.refreshButtonPRD,proportion=0,flag=wx.ALL,border=5)
        self.sizerPRDTableDisplay.Add(self.inforalgoGridPRD,proportion=0,flag=wx.ALL,border=5)
        #INFORALGO TABLE DISPLAY
        self.boxTableDisplay = wx.StaticBox(self,label = 'Inforalgo UAT table contents')
        self.sizerTableDisplay = wx.StaticBoxSizer(self.boxTableDisplay,wx.VERTICAL)   
        self.refreshButton = wx.Button(self, label = "Refresh!")
        self.refreshButton.Bind(wx.EVT_BUTTON, self.onRefreshButton)
        self.inforalgoGrid = gridlib.Grid(self)
        self.inforalgoGrid.ShowScrollbars(wx.SHOW_SB_NEVER,wx.SHOW_SB_ALWAYS)
        self.inforalgoGridRows = 10
        self.inforalgoGrid.CreateGrid(self.inforalgoGridRows,9)#one extra empty col at the end
        self.inforalgoGrid.SetColSize(8,20)#that extra col is small, the scrollbar will be on top
        self.inforalgoGrid.EnableEditing = False
        inforalgoGridCols = ['bbrgDate','bbrgTime','bbrgStatus','bbrgSec6id','bbrgVala','bbrgValc','bbrgValb','bbrgVald']
        for (i,h) in enumerate(inforalgoGridCols):
            self.inforalgoGrid.SetColLabelValue(i,h)
        self.sizerTableDisplay.Add(self.refreshButton,proportion=0,flag=wx.ALL,border=5)
        self.sizerTableDisplay.Add(self.inforalgoGrid,proportion=0,flag=wx.ALL,border=5)
        #PUT IT ALL TOGETHER
        self.hSizerSingleRecord = wx.BoxSizer(wx.HORIZONTAL)
        self.hSizerSingleRecord.Add(self.sizerAddRecord, 1, wx.ALL|wx.EXPAND, 10)
        self.hSizerSingleRecord.Add(self.sizerDeleteRecord, 1, wx.ALL|wx.EXPAND, 10)
        self.topSizer.Add(self.hSizerSingleRecord, 0, wx.ALL|wx.EXPAND, 10)
        # self.topSizer.Add(self.sizerAddRecord, 0, wx.ALL|wx.EXPAND, 10)
        # self.topSizer.Add(self.sizerDeleteRecord, 0, wx.ALL|wx.EXPAND, 10)
        self.hSizerPricerRecord = wx.BoxSizer(wx.HORIZONTAL)
        self.hSizerPricerRecord.Add(self.sizerAddPricerRecords, 1, wx.ALL|wx.EXPAND, 10)
        self.hSizerPricerRecord.Add(self.sizerDeletePricerRecords, 1, wx.ALL|wx.EXPAND, 10)
        self.topSizer.Add(self.hSizerPricerRecord, 0, wx.ALL|wx.EXPAND, 10)
        # self.topSizer.Add(self.sizerAddPricerRecords, 0, wx.ALL|wx.EXPAND, 10)
        # self.topSizer.Add(self.sizerDeletePricerRecords, 0, wx.ALL|wx.EXPAND, 10)
        self.topSizer.Add(self.sizerDeleteAllRecords, 0, wx.ALL|wx.EXPAND, 10)
        self.hSizerUpdatePrices = wx.BoxSizer(wx.HORIZONTAL)
        self.hSizerUpdatePrices.Add(self.sizerUpdateTimeStampsRecords, 1, wx.ALL|wx.EXPAND, 10)
        self.hSizerUpdatePrices.Add(self.sizerUpdateFromTable, 1, wx.ALL|wx.EXPAND, 10)
        self.topSizer.Add(self.hSizerUpdatePrices, 0, wx.ALL|wx.EXPAND, 10)
        self.topSizer.Add(self.sizerPRDTableDisplay, 0, wx.ALL|wx.EXPAND, 10)
        self.topSizer.Add(self.sizerTableDisplay, 0, wx.ALL|wx.EXPAND, 10)
        self.SetSizer(self.topSizer)
        self.Layout()

    def onIsinAddButton(self, event):
        isin = self.inputGrid.GetCellValue(0,0)
        bid_price = float(self.inputGrid.GetCellValue(0,1))
        ask_price = float(self.inputGrid.GetCellValue(0,2))
        bid_size = float(self.inputGrid.GetCellValue(0,3))
        ask_size = float(self.inputGrid.GetCellValue(0,4))
        try:
            self.uat_table.insert_record(isin, bid_price, ask_price, bid_size*1000, ask_size*1000)
            print 'UAT inserted ' + isin
        except:
            print 'UAT possible error inserting ' + isin
        try:            
            self.prd_table.insert_record(isin, bid_price, ask_price, bid_size*1000, ask_size*1000)
            print 'PRD inserted ' + isin
        except:
            print 'PRD possible error inserting ' + isin
        # try:
        #     self.uat_table.insert_record(isin, bid_price, ask_price, bid_size*1000, ask_size*1000)
        #     print 'Inserted ' + isin
        # except:
        #     print 'Failed to insert price for ' + isin
        # try:
        #     self.prd_table.insert_record(isin, bid_price, ask_price, bid_size*1000, ask_size*1000)
        #     print 'PRD inserted ' + isin
        # except:
        #     print 'PRD failed to insert price for ' + isin
        pass

    def onIsinDeleteButton(self, event):
        self.uat_table.delete_record(self.isinDeleteCtrl.GetValue())
        self.prd_table.delete_record(self.isinDeleteCtrl.GetValue())
        self.onRefreshButton(event)
        pass

    def onAddPricerRecordsButton(self, event):
        df = self.uat_table.read_table()
        df_prd = self.prd_table.read_table()
        existing_isins = list(df['bbrgSec6id'])
        existing_isins_prd = list(df_prd['bbrgSec6id'])
        for (i,bonddata) in self.bdm.df.iterrows():
            if bonddata['ISIN'] not in existing_isins:
                try:
                    self.uat_table.insert_record(bonddata['ISIN'], bonddata['BID'], bonddata['ASK'], int(bonddata['BID_SIZE']), int(bonddata['ASK_SIZE']))
                except:
                    print 'Error adding ' + bonddata['ISIN']
        for (i,bonddata) in self.bdm.df.iterrows():
            if bonddata['ISIN'] not in existing_isins_prd:
                try:
                    self.prd_table.insert_record(bonddata['ISIN'], bonddata['BID'], bonddata['ASK'], int(bonddata['BID_SIZE']), int(bonddata['ASK_SIZE']))
                except:
                    print 'PRD error adding ' + bonddata['ISIN']
        self.onRefreshButton(event)

    def onDeletePricerRecordsButton(self,event):
        for (i,bonddata) in self.bdm.df.iterrows():
            self.uat_table.delete_record(bonddata['ISIN'])
            self.prd_table.delete_record(bonddata['ISIN'])
        self.onRefreshButton(event)

    def onDeleteAllRecordsButton(self,event):
        if self.isinDeleteAllCtrl.GetValue().encode('hex') == '4963426353':#decode this to find out password
            self.uat_table.empty_table()
            self.prd_table.empty_table()
        self.onRefreshButton(event)

    def onUpdateTimeStampsRecordsButton(self,event):
        for (i,bonddata) in self.bdm.df.iterrows():
            if bonddata['BID_SIZE'] != 0 or bonddata['ASK_SIZE'] != 0:
                try:
                    self.uat_table.send_price(bonddata['ISIN'], bonddata['BID'], bonddata['ASK'], int(float(bonddata['BID_SIZE'])), int(float(bonddata['ASK_SIZE'])))
                except:
                    print 'Failed to send price for ' + bonddata['ISIN']
                try:
                    self.prd_table.send_price(bonddata['ISIN'], bonddata['BID'], bonddata['ASK'], int(float(bonddata['BID_SIZE'])), int(float(bonddata['ASK_SIZE'])))
                except:
                    print 'PRD failed to send price for ' + bonddata['ISIN']
        self.onRefreshButton(event)

    def onUpdateFromTableButton(self,event):
        '''
        This will only push data if it's in the Inforalgo table AND in the Pricer.
        '''
        df = self.uat_table.read_table()
        df_prd = self.prd_table.read_table()
        bdm_isins = list(self.bdm.df['ISIN'])
        for (i,row) in df.iterrows():
            if row['bbrgSec6id'] in bdm_isins:
                try:
                    self.uat_table.send_price(row['bbrgSec6id'], float(row['bbrgVala']), float(row['bbrgValc']), int(float(row['bbrgValb'])), int(float(row['bbrgVald'])))
                except:
                    print 'Failed to send price for ' + row['bbrgSec6id']
        for (i,row) in df_prd.iterrows():
            if row['bbrgSec6id'] in bdm_isins:
                try:
                    self.prd_table.send_price(row['bbrgSec6id'], float(row['bbrgVala']), float(row['bbrgValc']), int(float(row['bbrgValb'])), int(float(row['bbrgVald'])))
                except:
                    print 'PRD failed to send price for ' + row['bbrgSec6id']
        pass

    def onRefreshButtonPRD(self, event):
        self.inforalgoGridPRD.ClearGrid()
        df = self.prd_table.read_table()
        if df.shape[0] > self.inforalgoGridRowsPRD:
            self.inforalgoGridPRD.AppendRows(df.shape[0]-self.inforalgoGridRowsPRD)
            self.inforalgoGridRowsPRD = df.shape[0]
        for row in df.itertuples():
            self.inforalgoGridPRD.SetCellValue(row[0],0,str(row[1]).strip())
            self.inforalgoGridPRD.SetCellValue(row[0],1,str(row[2]).strip())
            self.inforalgoGridPRD.SetCellValue(row[0],2,str(row[3]).strip())
            self.inforalgoGridPRD.SetCellValue(row[0],3,str(row[4]).strip())
            self.inforalgoGridPRD.SetCellValue(row[0],4,str(row[5]).strip())
            self.inforalgoGridPRD.SetCellValue(row[0],5,str(row[6]).strip())
            self.inforalgoGridPRD.SetCellValue(row[0],6,str(row[7]).strip())
            self.inforalgoGridPRD.SetCellValue(row[0],7,str(row[8]).strip())
        self.Refresh()
        pass

    def onRefreshButton(self, event):
        self.inforalgoGrid.ClearGrid()
        df = self.uat_table.read_table()
        if df.shape[0] > self.inforalgoGridRows:
            self.inforalgoGrid.AppendRows(df.shape[0]-self.inforalgoGridRows)
            self.inforalgoGridRows = df.shape[0]
        for row in df.itertuples():
            self.inforalgoGrid.SetCellValue(row[0],0,str(row[1]).strip())
            self.inforalgoGrid.SetCellValue(row[0],1,str(row[2]).strip())
            self.inforalgoGrid.SetCellValue(row[0],2,str(row[3]).strip())
            self.inforalgoGrid.SetCellValue(row[0],3,str(row[4]).strip())
            self.inforalgoGrid.SetCellValue(row[0],4,str(row[5]).strip())
            self.inforalgoGrid.SetCellValue(row[0],5,str(row[6]).strip())
            self.inforalgoGrid.SetCellValue(row[0],6,str(row[7]).strip())
            self.inforalgoGrid.SetCellValue(row[0],7,str(row[8]).strip())
        self.Refresh()
        pass


###BELOW USED FOR DEBUGGING SO FILE CAN BE SELF-CONTAINED###

class InforalgoControlFrame(wx.Frame):
    def __init__(self):
        wx.Frame.__init__(self, None, wx.ID_ANY, "Inforalgo control panel",size=(1280,850))
        uat_table = inforalgo.SQLTable(inforalgo.UAT_SERVER_CONNECTION_STRING)
        prd_table = inforalgo.SQLTable(inforalgo.PRD_SERVER_CONNECTION_STRING)
        self.panel = InforalgoControlPanel(self, uat_table=uat_table, prd_table=prd_table)


if __name__ == "__main__":
    app = wx.App()
    frame = InforalgoControlFrame().Show()
    app.MainLoop()

