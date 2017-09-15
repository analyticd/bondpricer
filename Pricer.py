"""
Pricer Window - Launches the pricer.

Written by Alexandre Almosni   alexandre.almosni@gmail.com
(C) 2015-2017 Alexandre Almosni
Released under Apache 2.0 license. More info at http://www.apache.org/licenses/LICENSE-2.0

**Steps to disable tabs for debugging:
    In PricerMenu.__init__, look for #grid_labels = ['Africa']# used for testing
    and uncomment - this will reduce the number of bonds loaded for testing

Classes:
    PricingGrid
    RunsGrid
    PricerWindow

Functions:
    send_mail_via_com()

"""


import wx
import wx.grid as gridlib
import pandas
import datetime
import win32com.client
import time
import wx.lib.colourdb
import wx.lib.pubsub
from wx.lib.pubsub import pub
#import warnings
#warnings.filterwarnings('error', category=UnicodeWarning)
#warnings.filterwarnings('error', message='*equal comparison failed*')
import inforalgo
import inforalgopanel
from subprocess import Popen
from win32api import GetUserName


from StaticDataImport import bonds, DEFPATH, APPPATH, bondRuns, frontToEmail, SPECIALBONDS, colFormats, runTitleStr, regsToBondName, tabList, columnListByTrader
from BondDataModel import BondDataModel

class MessageContainer():
    def __init__(self, data):
        self.data = data


def send_mail_via_com(text, subject, recipient, a1=None, a2=None):
    """Function to send email to bloomberg when users click on 'send' in the runs menu.
    Function is called by RunsGrid.sendRun()

    Keyword arguments: 
    text : Text message 
    subject : Email subject
    recipient : Recipient of email 
    a1 : attachment (False by default)
    a2 : attachment (False by default)
    """
    # s = win32com.client.Dispatch("Mapi.Session") works for Outlook 2003
    o = win32com.client.Dispatch("Outlook.Application")
    # s.Logon('Outlook') works for Outlook 2003
    # Msg = o.CreateItem(0) works for Outlook 2003
    Msg = o.CreateItem(0x0)  # works for Outlook 2007
    Msg.To = recipient
    Msg.Subject = subject
    Msg.Body = text
    if a1 is not None:
        Msg.Attachments.Add(a1)
    if a2 is not None:
        Msg.Attachments.Add(a2)
    Msg.Send()
    pass


class TextDisplayWindow(wx.Frame):
    def __init__(self, title, filename):
        wx.Frame.__init__(self, None, wx.ID_ANY, title, size=(800, 600))
        panel = wx.Panel(self) 
        multiText = wx.TextCtrl(panel, -1,"",size=(775, 575), style=wx.TE_MULTILINE|wx.TE_READONLY)#775 and 575 to leave space for the scrollbar
        f = open(APPPATH+filename, 'r') 
        multiText.SetValue(f.read())  #open the file from location as read
        f.close()
        multiText.SetFont(wx.Font(multiText.GetFont().GetPointSize(), wx.TELETYPE, wx.NORMAL, wx.NORMAL))
        self.Show()


class AxeGrid(wx.Frame):
    def __init__(self, title, bdm, bondList):
        wx.Frame.__init__(self, None, wx.ID_ANY, title, size=(800, 600))
        panel = wx.Panel(self) 
        sizer = wx.BoxSizer()

        self.grid = gridlib.Grid(panel)
        self.grid.Bind(wx.EVT_KEY_DOWN, self.onKeyDown)
        sizer.Add(self.grid, proportion=1, flag=wx.EXPAND)
        #Attributes creation
        self.fontBold = self.grid.GetDefaultCellFont()
        self.fontBold.SetWeight(wx.FONTWEIGHT_BOLD)
        defattr = wx.grid.GridCellAttr()
        defattr.SetReadOnly(True)
        bidaskinputattr = wx.grid.GridCellAttr()
        bidaskinputattr.SetReadOnly(False)
        bidaskinputattr.SetAlignment(wx.ALIGN_RIGHT, wx.ALIGN_CENTRE)
        bidaskinputattr.SetFont(self.fontBold)
        bidaskinputattr.SetTextColour(wx.BLUE)
        bidasksizeinputattr = wx.grid.GridCellAttr()
        bidasksizeinputattr.SetReadOnly(False)
        bidasksizeinputattr.SetAlignment(wx.ALIGN_RIGHT, wx.ALIGN_CENTRE)
        bidasksizeinputattr.SetFont(self.fontBold)

        self.columnList = ['Security', 'B Axe', 'B Sz(M)', 'B Px', 'A Axe', 'A Sz(M)', 'A Px']

        self.grid.CreateGrid(200, len(self.columnList))
        bidasksizeinputattr.SetTextColour(wx.BLUE)
        
        self.grid.SetColAttr(0, defattr)
        self.grid.SetColSize(0, 100)
        self.grid.SetColAttr(1, defattr)
        self.grid.SetColSize(1, 50)
        self.grid.SetColAttr(2, bidasksizeinputattr)
        self.grid.SetColSize(2, 50)
        self.grid.SetColAttr(3, bidaskinputattr)
        self.grid.SetColSize(3, 50)
        self.grid.SetColAttr(4, defattr)
        self.grid.SetColSize(4, 50)
        self.grid.SetColAttr(5, bidasksizeinputattr)
        self.grid.SetColSize(5, 50)
        self.grid.SetColAttr(6, bidaskinputattr)
        self.grid.SetColSize(6, 50)

        self.grid.SetRowLabelSize(100)

        for (j, header) in enumerate(self.columnList):
            self.grid.SetColLabelValue(j, header)

        i = 0
        for bond in bondList:
            if bond not in bdm.df.index:
                continue
            if bdm.df.at[bond,'POSITION']==0:
                continue
            self.grid.SetRowLabelValue(i, bond)
            self.grid.SetCellValue(i, 0, bdm.df.at[bond, 'ISIN'])
            if bdm.df.at[bond,'POSITION']<0:
                self.grid.SetCellValue(i, 1, 'Y')
                self.grid.SetCellValue(i, 2, '{:.0f}'.format(-bdm.df.at[bond, 'POSITION']/1000.))
                self.grid.SetCellValue(i, 3, '{:,.3f}'.format(bdm.df.at[bond, 'BID']))
            if bdm.df.at[bond,'POSITION']>0:
                self.grid.SetCellValue(i, 4, 'Y')
                self.grid.SetCellValue(i, 5, '{:.0f}'.format(bdm.df.at[bond, 'POSITION']/1000.))
                self.grid.SetCellValue(i, 6, '{:,.3f}'.format(bdm.df.at[bond, 'ASK']))
            i = i + 1
        panel.SetSizerAndFit(sizer)
        self.Show()
        pass

    def onCopySelection(self):
    # Number of rows and cols
        if self.grid.GetSelectionBlockTopLeft() == []:
            rows = 1
            cols = 1
            iscell = True
        else:
            rows = self.grid.GetSelectionBlockBottomRight()[0][0] - self.grid.GetSelectionBlockTopLeft()[0][0] + 1
            cols = self.grid.GetSelectionBlockBottomRight()[0][1] - self.grid.GetSelectionBlockTopLeft()[0][1] + 1
            iscell = False
        # data variable contain text that must be set in the clipboard
        data = ''
        # For each cell in selected range append the cell value in the data variable
        # Tabs '\t' for cols and '\r' for rows
        for r in range(rows):
            for c in range(cols):
                if iscell:
                    data += str(self.grid.GetCellValue(self.grid.GetGridCursorRow() + r, self.grid.GetGridCursorCol() + c))
                else:
                    data += str(self.grid.GetCellValue(self.grid.GetSelectionBlockTopLeft()[0][0] + r, self.grid.GetSelectionBlockTopLeft()[0][1] + c))
                if c < cols - 1:
                    data += '\t'
            data += '\n'
        # Create text data object
        clipboard = wx.TextDataObject()
        # Set data object value
        clipboard.SetText(data)
        # Put the data in the clipboard
        if wx.TheClipboard.Open():
            wx.TheClipboard.SetData(clipboard)
            wx.TheClipboard.Close()
        else:
            wx.MessageBox("Can't open the clipboard", "Error")

    def onKeyDown(self, event):
        keycode = event.GetKeyCode()
        if keycode == 67 and event.ControlDown():
            self.onCopySelection()
        else:
            pass
        event.Skip() # important, otherwise one would need to define all possible events


class RunsGrid(gridlib.Grid):
    """RunsGrid Class: Class to define the RunsGrid tab.

    Also creates top row to allow users to send runs for specific bonds. When values in the cells are changed,
    an event is sent out to add bonds into self.df.

    Attributes:
    self.df : Pandas DataFrame consisting of the run definitions.
    self.bdm : Class instance of BondDataModel. 

    Methods:
    __init__() 
    onReloadRunDefinitions() : Reload run definitions when the 'Reload run definitions' button is clicked.
    fillGrid() : Populates wx.Grid.
    onDoubleClick() : Event handler when user doubleclicks.
    sendRun() : Function to send run to user's Bloomberg's email.
    addBondsToRuns() : Registers a custom bond when users enters a bond in the top row of the runs tab.

    """
    def __init__(self, panel, df, bdm, pricerwindow):
        """
        Keyword arguments:
        panel : wx.Panel object
        df : pandas.DataFrame 
        bdm : BondDataModel class instance 

        By default the grid will have 100 lines and 60 rows (max 60 runs, 100 securities per run).

        """
        gridlib.Grid.__init__(self, panel)
        self.CreateGrid(60,100) 
        self.defaultFont = self.GetDefaultCellFont()
        self.fontBold = self.GetDefaultCellFont()
        self.fontBold.SetWeight(wx.FONTWEIGHT_BOLD)
        self.df = df
        self.bdm = bdm
        self.pricerwindow = pricerwindow
        self.SetRowLabelSize(80)
        self.SetColLabelSize(50)
        self.SetColLabelValue(0,'Double click \n to send')
        self.SetColSize(1,200)
        self.SetColLabelValue(1, 'Header: ')
        self.SetColLabelValue(2, 'Daily change: ')
        self.SetColLabelValue(3, 'Autoforward: ')
        self.fillGrid()
        self.Bind(wx.grid.EVT_GRID_CELL_LEFT_DCLICK, self.onDoubleClick)

    def onReloadRunDefinitions(self, event):
        '''
        Reload the run definitions (DEFPATH+'run.csv') when the 'Reload run definitions' is clicked.         
        '''
        self.df = pandas.read_csv(DEFPATH+'runs.csv', index_col=0)
        self.ClearGrid()
        self.fillGrid()
        wx.CallAfter(self.ForceRefresh)

    def fillGrid(self):
        '''
        Function to populate the wx.Grid with data in self.df. fillGrid detects the inputs selected by the user,
        namely:
        1) Price/ Yield/ Z-Spread 
        2) True/ False (for autoforwarding)
        Fonts in blue represents fields that can be changed by the user.
        '''
        self.df = pandas.read_csv(DEFPATH+'runs.csv', index_col=0)
        maxCol = max (self.df.iloc[i].count() for i in range(len(self.df.index)))        
        #Sets columns
        for (j, header) in enumerate(self.df.columns):
            self.SetColLabelValue(j + 1, header)
        #Sets row
        for (k, header) in enumerate(self.df.index): #K=Row
            self.SetRowLabelValue(k, header)    #Bond Labels
            self.SetCellValue(k, 0, 'SEND')     #Send button
            self.SetReadOnly(k, 0, True)
            self.SetCellFont(k, 0, self.fontBold)
            self.SetCellTextColour(k,0,wx.BLUE)
            for i in range(maxCol):             #F = Col
                if pandas.isnull(self.df.iloc[k,i]):
                    value = ''
                else:
                    if (i != 1) and (i != 2):
                        self.SetReadOnly(k, i + 1, True)
                    else:
                        self.SetReadOnly(k, i + 1, False)
                        self.SetCellTextColour(k, i + 1, wx.BLUE)
                        if i == 1: #Drop down list. Options: Price/ Yield/ Z-Spread
                            self.SetCellEditor(k, i + 1, wx.grid.GridCellChoiceEditor(['Price','Yield','Spread'],True))
                        if i == 2: #Drop down list. Options: True/ False
                            self.SetCellEditor(k, i + 1, wx.grid.GridCellChoiceEditor(['True','False'],True))
                    value = str(self.df.iloc[k,i])
                    self.SetCellValue(k, i + 1, value)
                    if value == 'START' or value == 'END':
                        self.SetCellFont(k, i + 1, self.fontBold)
                    else:
                        self.SetCellFont(k, i + 1, self.defaultFont)
            if k==0:    #For the first row => where user specify individual bonds.
                #Sends out an event when the values in the cells are changed.
                self.Bind(gridlib.EVT_GRID_CELL_CHANGE, self.addBondsToRuns)
                self.SetReadOnly(k, 1, False)
                self.SetCellTextColour(k, 1, wx.BLUE)
                for i in range(5, len(self.df.iloc[0])):
                    self.SetReadOnly(k, i, False)
                    self.SetCellTextColour(k, i, wx.BLUE)
                    
    def addBondsToRuns(self,event):
        """
        Function to add bonds to self.df when user inputs bonds in the top row. Bolds the value if it == START or END.
        """
        row = 0
        col = event.GetCol()
        value = str(self.GetCellValue(row,col))
        #Adds the value to self.df
        self.df.iloc[0,col-1] = value

        #If value == START or END, make it bold
        if value == 'START' or value == 'END':
            self.SetCellFont(row,col,self.fontBold)
        else:
            self.SetCellFont(row,col,self.defaultFont)

    def onDoubleClick(self,event):
        '''
        ActionHandler for a double click event. Triggers the sendRun() function.

        Parameters passed to sendRun:
            dailyChange: Sets daily change field to the 2 column of the clicked row
            autoFwd: Sets True/False for autoforwarding to the value in the 3 column of the clicked row
            bondCol: Pandas Series. List of bonds to be queried and sent emailed to user.
        
        '''
        row = event.GetRow()
        col = event.GetCol()
        if col == 0:
            self.SetCellBackgroundColour(row, col, wx.RED)
            self.ForceRefresh()
            dailyChange = self.GetCellValue(row, 2)
            if dailyChange == 'Spread':
                #tdelta = datetime.datetime.now() - self.bdm.USDswapRate.lastRefreshTime
                if (datetime.datetime.now() - self.bdm.lastRefreshTime).seconds >= 7200: # HARD-CODING TWO HOURS IN SECONDS
                    dlg = wx.MessageDialog(self, 'Z-spreads last full update >2h ago. Do you want to refresh first?', 'Z-spread update alert', style=wx.YES_NO)
                    #answer = dlg.ShowModal()
                    if dlg.ShowModal() == wx.ID_YES:
                        self.pricerwindow.onRefreshSwapRates(event)
            autoFwd = self.GetCellValue(row, 3)
            if row == 0:
                bondCol=pandas.Series(index=self.df.iloc[0].index)
                for i in range(0, self.df.iloc[0].count()):
                    bondCol.iloc[i] = self.GetCellValue(row,i+1)
            else:
                bondCol = self.df.iloc[row]
            self.sendRun(bondCol, autoFwd, dailyChange)
            wx.CallLater(500, self.SetCellBackgroundColour, row, col, wx.WHITE)
            wx.CallLater(600, self.ForceRefresh)
        else:
            pass

    def sendRun(self, bondCol, autoFwd, dailyChange):
        '''
        Function to extract the information queried, then triggers send_mail_via_com() to send the information
        to the user. sendRun is called by onDoubleClick(). 

        Keyword arguments:
            bondCol: list of bonds to be queried
            autoFwd: True/False for autoforwarding
            dailyChange: Can either be Price/ Yield/ or Spread

        '''
        strHeader = 'Ccy Security                  B Px A Px     B YTM A YTM  B ZS A ZS'
        if dailyChange == 'Price':
            strHeader = strHeader + "  PChgD PChgW"
        elif dailyChange == 'Spread':
            strHeader = strHeader + "  ZChgD ZChgW"
        else:
            strHeader = strHeader + "  YChgD YChgW"
        strHeader = strHeader + '\n' + "-------------------------------------------------------------------------------" + '\n'
        strRunOutput = ''
        for i in range(4, bondCol.shape[0]):
            bond = bondCol.iloc[i]
            #The line below makes it works regardless whether user types 'END' in the top columns.
            if bond == 'END' or type(bond) == float:
                break
            strPrice = '{:>7.3f}'.format(self.bdm.df.at[bond, 'BID']) + "-" + '{:<7.3f}'.format(
                self.bdm.df.at[bond, 'ASK'])
            strYield = '{:>5.2f}'.format(self.bdm.df.at[bond, 'YLDB']) + "/" + '{:<5.2f}'.format(
                self.bdm.df.at[bond, 'YLDA'])
            strBidAskZ = '{:>4.0f}'.format(self.bdm.df.at[bond, 'ZB']) + "/" + '{:<4.0f}'.format(
                self.bdm.df.at[bond, 'ZA'])

            if len(strYield) > 11:
                strYield = '  nan/nan  '
            if len(strBidAskZ) > 9:
                strBidAskZ = ' nan/nan '

            strLine = self.bdm.df.at[bond, 'CRNCY'] + ' ' + self.bdm.df.at[bond, 'SECURITY_NAME'].ljust(
                23) + strPrice + '  ' + strYield + '  ' + strBidAskZ + '  '

            if dailyChange == 'Price':
                strChange = '{: >+5.2f}'.format(self.bdm.df.at[bond, 'DP1D']) + "/" + '{: <+5.2f}'.format(
                    self.bdm.df.at[bond, 'DP1W'])
            elif dailyChange == 'Spread':
                strChange = '{: >+5.0f}'.format(self.bdm.df.at[bond, 'DISP1D']) + "/" + '{: <+5.0f}'.format(
                    self.bdm.df.at[bond, 'DISP1W'])
            else: #Yield
                strChange = '{: >+5.0f}'.format(self.bdm.df.at[bond, 'DY1D']) + "/" + '{: <+5.0f}'.format(
                    self.bdm.df.at[bond, 'DY1W'])
            
            if len(strChange) > 11:
                strChange = '  nan/nan  '
            strLine = strLine + strChange
            strLine = strLine + '\n'
            strRunOutput = strRunOutput + strLine
        strRunOutput = strHeader + strRunOutput + '\n\n\n'
        if autoFwd == 'TRUE' or autoFwd == 'True':  ##Excel will mess it up when updating the file
            strRunOutput = strRunOutput + '#autoforward' + '\n\n\n'
        strRunOutput = strRunOutput + '#icbcsrun' + '\n\n\n'
        send_mail_via_com(strRunOutput, runTitleStr + ' - ' + bondCol['Header'],
                          frontToEmail[self.bdm.mainframe.front_username])





class GenericPricingGrid(gridlib.Grid):
    def __init__(self):
        pass

class PricingGrid(gridlib.Grid):
    """PricingGrid class : Class to define the pricing grid

    Attributes:
    self.tab : pandas.DataFrame containing the names of the tabs to be created 
    self.bondList : list of bonds 
    self.columnList : list of columns 
    self.bondsWithBenchmark : list of bonds with Benchmarks
    self.bdm : BondDataModel class instance
    self.daysToCouponWarning : warning threshhold for days to coupon  

    Methods: 
    __init__()
    initialPaint() : Function to paint the background colour orange when Pricer is first loaded.
    showPopUpMenu() : Create and display a popup menu on right-click event: 
    showTradeHistory() : Shows the TradeHistory 
    copyLine() : Copies the selected line 
    copyISIN() : Copies the ISIN of the selected bond
    showDES() : Shows the description on Bloomberg
    showCN() : Shows the company news on bloomberg 
    showGP() : Shows the price graph on bloomberg 
    showALLQ() : Shows ALLQ on bloomberg 
    bbgScreenSendKeys() : Sends shell command to bloomberg.
    updateBenchmarks() : updates benchmarks 
    updateOneBenchmark() : Updates single benchmark 
    singleBenchmarkUpdate(): Updates bond in the benchmark 
    updatePositions() : Holding function to only update positions after thread has died.
    updateAllPositions() : Updates all the position 
    updateLine() : Holding function to only update line after thread has died.
    updateLineAction() : Updates each line 
    createField() : Creates the fields to be displayed

    ---------------------
    Back to RunsGrid
    Back to PricerWindow
    ---------------------    
    """
    def __init__(self, panel, tab, columnList, bdm, pricer):
        """
        Init function defines columns attributes and binds right click event to the grids.

        Keyword arguments:
        panel : wx.Panel object
        tab : pandas.DataFrame containing the names of the tabs to be created 
        columnList : list of columns
        bdm : BondDataModel class instance
        """
        gridlib.Grid.__init__(self, panel)
        #Attributes creation
        self.fontBold = self.GetDefaultCellFont()
        self.fontBold.SetWeight(wx.FONTWEIGHT_BOLD)
        defattr = wx.grid.GridCellAttr()
        defattr.SetReadOnly(True)
        bidaskattr = wx.grid.GridCellAttr()
        bidaskattr.SetAlignment(wx.ALIGN_RIGHT, wx.ALIGN_CENTRE)
        bidaskattr.SetFont(self.fontBold)
        bidaskattr.SetReadOnly(True)
        rightalignattr = wx.grid.GridCellAttr()
        rightalignattr.SetAlignment(wx.ALIGN_RIGHT, wx.ALIGN_CENTRE)
        rightalignattr.SetReadOnly(True)
        centrealignattr = wx.grid.GridCellAttr()
        centrealignattr.SetAlignment(wx.ALIGN_CENTRE, wx.ALIGN_CENTRE)
        centrealignattr.SetReadOnly(True)
        bidaskinputattr = wx.grid.GridCellAttr()
        bidaskinputattr.SetReadOnly(False)
        bidaskinputattr.SetAlignment(wx.ALIGN_RIGHT, wx.ALIGN_CENTRE)
        bidaskinputattr.SetFont(self.fontBold)
        bidaskinputattr.SetTextColour(wx.BLUE)
        bidasksizeinputattr = wx.grid.GridCellAttr()
        bidasksizeinputattr.SetReadOnly(False)
        bidasksizeinputattr.SetAlignment(wx.ALIGN_RIGHT, wx.ALIGN_CENTRE)
        bidasksizeinputattr.SetFont(self.fontBold)
        sendattr = wx.grid.GridCellAttr()
        sendattr.SetTextColour(wx.BLUE)
        sendattr.SetAlignment(wx.ALIGN_RIGHT, wx.ALIGN_CENTRE)
        sendattr.SetReadOnly(True)
        sendattr.SetFont(self.fontBold)

        self.daysToCouponWarning = 10
        self.clickedISIN = ''
        self.clickedBond = ''
        self.tabKeyCounter = 0

        pub.subscribe(self.updateLine, "BOND_PRICE_UPDATE")
        pub.subscribe(self.updatePositions, "POSITION_UPDATE")
        pub.subscribe(self.updateBGNPrices, "BGN_PRICE_UPDATE")

        self.tab = tab
        self.bondList = list(self.tab['Bonds'])
        self.columnList = columnList
        self.bondsWithBenchmark = list(self.tab[self.tab['Benchmarks'].notnull()]['Bonds'])

        self.bondToBenchmark = self.tab.loc[self.tab['Benchmarks'].notnull(),['Bonds','Benchmarks']].set_index('Bonds')['Benchmarks'].to_dict()

        self.bdm = bdm
        self.pricer = pricer
        self.CreateGrid(len(self.bondList), len(self.columnList))

        colFormats['wxFormat'] = pandas.np.nan
        if self.pricer.mainframe is None or self.pricer.mainframe.isTrader:        
            colFormats.loc[colFormats['Format']=='BIDASK', 'wxFormat'] = bidaskinputattr
            bidasksizeinputattr.SetTextColour(wx.BLUE)
        else:
            colFormats.loc[colFormats['Format']=='BIDASK', 'wxFormat'] = bidaskattr
        colFormats.loc[colFormats['Format']=='CENTRE', 'wxFormat'] = centrealignattr
        colFormats.loc[colFormats['Format']=='RIGHT', 'wxFormat'] = rightalignattr
        colFormats.loc[colFormats['Format']=='DEFAULT', 'wxFormat'] = defattr
        colFormats.loc[colFormats['Format']=='BIDASKINPUT', 'wxFormat'] = bidaskinputattr
        colFormats.loc[colFormats['Format']=='BIDASKSIZEINPUT', 'wxFormat'] = bidasksizeinputattr
        for c in self.columnList:
            if c in colFormats.index:
                self.SetColAttr(self.columnList.index(c), colFormats.loc[c, 'wxFormat'])
                self.SetColSize(self.columnList.index(c), colFormats.loc[c, 'Width'])

        self.SetRowLabelSize(1)

        self.Bind(gridlib.EVT_GRID_CELL_RIGHT_CLICK, self.showPopUpMenu)
        self.Bind(gridlib.EVT_GRID_CELL_CHANGE, self.onEditCell)
        self.Bind(wx.EVT_KEY_DOWN, self.onKeyDown)
        self.Bind(gridlib.EVT_GRID_SELECT_CELL, self.onSingleSelection)
        self.Bind(gridlib.EVT_GRID_RANGE_SELECT, self.onSelection)

        self.showAllqID = wx.NewId()
        self.showTradeHistoryID = wx.NewId()
        self.showDESID = wx.NewId()
        self.showCNID = wx.NewId()
        self.showGPID = wx.NewId()
        self.buyRegsID = wx.NewId()
        self.sellRegsID = wx.NewId()
        self.buy144AID = wx.NewId()
        self.sell144AID = wx.NewId()
        self.copyLineID = wx.NewId()
        self.copyISINID = wx.NewId()
        self.pastePricesID = wx.NewId()
        self.Bind(wx.EVT_MENU, self.showALLQ, id=self.showAllqID)
        self.Bind(wx.EVT_MENU, self.showTradeHistory, id=self.showTradeHistoryID)
        self.Bind(wx.EVT_MENU, self.showDES, id=self.showDESID)
        self.Bind(wx.EVT_MENU, self.showCN, id=self.showCNID)
        self.Bind(wx.EVT_MENU, self.showGP, id=self.showGPID)
        self.Bind(wx.EVT_MENU, self.buyRegs, id=self.buyRegsID)
        self.Bind(wx.EVT_MENU, self.sellRegs, id=self.sellRegsID)
        self.Bind(wx.EVT_MENU, self.buy144A, id=self.buy144AID)
        self.Bind(wx.EVT_MENU, self.sell144A, id=self.sell144AID)
        self.Bind(wx.EVT_MENU, self.copyLine, id=self.copyLineID)
        self.Bind(wx.EVT_MENU, self.copyISIN, id=self.copyISINID)
        self.Bind(wx.EVT_MENU, self.onPastePrices, id=self.pastePricesID)
        self.selected_row_number = 0
        self.selected_col_number = 0
        self.previousSingleSelection = True
        self.singleSelection = True

        self.askCol = self.columnList.index('ASK')

    def initialPaint(self):
        """
        Function to paint the background colour orange when Pricer is first loaded. Function is called by
        PricerWindow.
        Salespeople only see positions up to 1mm absolute size.
        """
        wx.lib.colourdb.updateColourDB()
        headerlineattr = wx.grid.GridCellAttr()
        headerlineattr.SetBackgroundColour(wx.NamedColour('CORNFLOWERBLUE'))
        headerlineattr.SetFont(self.fontBold)
        headerlineattr.SetReadOnly(True)

        self.oddLineColour = wx.NamedColour('GAINSBORO')
        self.oddlineattr = wx.grid.GridCellAttr()
        self.oddlineattr.SetBackgroundColour(self.oddLineColour)

        for (j, header) in enumerate(self.columnList):
            self.SetColLabelValue(j, header)
            for (i, bond) in enumerate(self.bondList):
                if bond in self.bdm.df.index:
                    if i % 2:
                        self.SetRowAttr(i,self.oddlineattr.Clone())#this clone thing is needed in wxPython 3.0 (worked fine without in 2.8)
                    if header in self.bdm.df.columns:
                        value = self.bdm.df.at[bond, header]
                        if header == 'POSITION':
                            if self.bdm.mainframe is None or self.bdm.mainframe.isTrader:
                                value = '{:,.0f}'.format(value)
                            else:
                                if value > 1000000:
                                    value = '>1MM'
                                elif value < -1000000:
                                    value = '<-1MM'
                                else:
                                    value = '{:,.0f}'.format(value)
                        elif header == 'SIZE':
                            value = '{:,.0f}'.format(value / 1000000) + 'm'
                        else:
                            value = str(value)
                        self.SetCellValue(i, j, value)
                        if header == 'D2CPN' and self.bdm.df.at[bond, header] <= self.daysToCouponWarning:
                            self.SetCellBackgroundColour(i, j, wx.RED)
                    if header in ['BID_S', 'ASK_S']:
                        self.SetCellValue(i,j,'{:,.0f}'.format(self.bdm.df.at[bond, header + 'IZE']/1000.))
                else:
                    if j == 0:
                        self.SetCellValue(i, j, bond)
                        if bond != '':
                            self.SetRowAttr(i, headerlineattr)

    def onKeyDown(self, event):
        keycode = event.GetKeyCode()
        if keycode == wx.WXK_TAB:
            self.tabKeyCounter = self.tabKeyCounter + 1
        elif keycode == wx.WXK_NUMPAD_ENTER or keycode == wx.WXK_RETURN:
            for i in range(0,self.tabKeyCounter):
                self.MoveCursorLeft(False)
            self.tabKeyCounter = 0
        elif keycode == wx.WXK_WINDOWS_MENU:
            self.clickedBond = self.GetCellValue(self.GetGridCursorRow(), self.columnList.index('BOND'))
            self.showPopUpMenu(event, True)
        elif keycode == 67 and event.ControlDown():
            self.onCopySelection()
        else:
            pass
        event.Skip() # important, otherwise one would need to define all possible events

    def onSingleSelection(self, event):
        if not (self.pricer.mainframe is None):
            bond = self.GetCellValue(event.GetRow(), 1)
            if bond in self.bdm.df.index and self.bdm.mainframe.isTrader:
                postxt = 'REGS: ' + '{:,.0f}'.format(self.bdm.df.at[bond, 'REGS']) + '    144A: '+ '{:,.0f}'.format(self.bdm.df.at[bond, '144A'])
                risktxt = 'SPV01: ' + '{:,.0f}'.format(self.bdm.df.at[bond, 'RISK'])
                wx.CallAfter(self.writeToStatusBar, bond + ':    ' + postxt + '    ' + risktxt)
        event.Skip()

    def onSelection(self, event):
        # issue is this and onSingleSelection are both fired concurrently
        self.previousSingleSelection = self.singleSelection
        if self.GetSelectionBlockTopLeft() == []:
            self.singleSelection = True
        else:
            self.singleSelection = False
            self.selected_row_number = self.GetSelectionBlockBottomRight()[0][0] - self.GetSelectionBlockTopLeft()[0][0] + 1
            self.selected_col_number = self.GetSelectionBlockBottomRight()[0][1] - self.GetSelectionBlockTopLeft()[0][1] + 1
        if self.selected_col_number == 1 and self.GetGridCursorCol() == self.columnList.index('POSITION') and self.bdm.mainframe.isTrader:
            rowstart = self.GetGridCursorRow()
            bondlist = [self.GetCellValue(rowstart + r, 1) for r in range(self.selected_row_number)]
            postxt = 'Position: ' + '{:,.0f}'.format(self.bdm.df.loc[bondlist,'POSITION'].sum())
            risktxt = 'SPV01: ' + '{:,.0f}'.format(self.bdm.df.loc[bondlist,'RISK'].sum())
            wx.CallAfter(self.writeToStatusBar, 'Sum:' + '    ' + postxt + '    ' + risktxt)
            pass

    def writeToStatusBar(self,txt):
        self.pricer.statusbar.SetStatusText(txt, 3)

    def onCopySelection(self):
        # Number of rows and cols
        #print self.GetSelectionBlockBottomRight()
        #print self.GetGridCursorRow()
        #print self.GetGridCursorCol()
        if self.GetSelectionBlockTopLeft() == []:
            rows = 1
            cols = 1
            iscell = True
        else:
            rows = self.GetSelectionBlockBottomRight()[0][0] - self.GetSelectionBlockTopLeft()[0][0] + 1
            cols = self.GetSelectionBlockBottomRight()[0][1] - self.GetSelectionBlockTopLeft()[0][1] + 1
            iscell = False
        # data variable contain text that must be set in the clipboard
        data = ''
        # For each cell in selected range append the cell value in the data variable
        # Tabs '\t' for cols and '\r' for rows
        for r in range(rows):
            for c in range(cols):
                if iscell:
                    data += str(self.GetCellValue(self.GetGridCursorRow() + r, self.GetGridCursorCol() + c))
                else:
                    data += str(self.GetCellValue(self.GetSelectionBlockTopLeft()[0][0] + r, self.GetSelectionBlockTopLeft()[0][1] + c))
                if c < cols - 1:
                    data += '\t'
            data += '\n'
        # Create text data object
        clipboard = wx.TextDataObject()
        # Set data object value
        clipboard.SetText(data)
        # Put the data in the clipboard
        if wx.TheClipboard.Open():
            wx.TheClipboard.SetData(clipboard)
            wx.TheClipboard.Close()
        else:
            wx.MessageBox("Can't open the clipboard", "Error")


    def onEditCell(self,event):
        if self.previousSingleSelection:
            self.onEditSingleCell(event)
        else:
            rowstart = event.GetRow()
            col = event.GetCol()
            colID = self.GetColLabelValue(col)
            strNewValue = self.GetCellValue(rowstart, col)
            self.onEditSingleCell(event)
            if self.selected_col_number == 1 and colID == 'BID':
                for r in range(self.selected_row_number):
                    if r==0:
                        continue#already done above
                    row = rowstart + r
                    bond = self.GetCellValue(row,1)
                    oldValue = float(self.GetCellValue(row,col))
                    newValue = self.readInput(oldValue,strNewValue)
                    self.SetCellValue(row,col,'{:,.3f}'.format(newValue))
                    try:
                        oldOffer = float(self.GetCellValue(row, self.askCol))
                    except:
                        oldOffer = 0
                    self.SetCellValue(row,self.askCol,'{:,.3f}'.format(newValue + oldOffer - oldValue))
                    self.sendUpdateToInforalgo(row)

    def onEditSingleCell(self,event):
        row = event.GetRow()
        col = event.GetCol()
        bond = self.GetCellValue(row,1)
        colID = self.GetColLabelValue(col)
        try:
            oldValue = float(event.GetString())
        except:
            oldValue = 0
        strNewValue = self.GetCellValue(row,col)
        newValue = self.readInput(oldValue,strNewValue)
        if colID == 'BID' or colID == 'ASK':
            self.SetCellValue(row, col, '{:,.3f}'.format(newValue))
        if colID == 'BID_S' or colID == 'ASK_S':
            self.SetCellValue(row, col, '{:,.0f}'.format(newValue))
        if colID == 'BID':
            try:
                oldOffer = float(self.GetCellValue(row, self.askCol))
            except:
                oldOffer = 0
            self.SetCellValue(row, self.askCol, '{:,.3f}'.format(newValue + oldOffer - oldValue))
        self.sendUpdateToInforalgo(row)

    def sendUpdateToInforalgo(self, row):
        wx.CallAfter(self.dataSentWarning,row)
        bbg_sec_id = self.GetCellValue(row,0)
        bid_price = float(self.GetCellValue(row, self.columnList.index('BID')))
        ask_price = float(self.GetCellValue(row, self.columnList.index('ASK')))
        try:
            bid_size = int(self.GetCellValue(row, self.columnList.index('BID_S')).replace(',',''))
            ask_size = int(self.GetCellValue(row, self.columnList.index('ASK_S')).replace(',',''))
        except:
            bid_size = 0
            ask_size = 0
        try:
            self.pricer.uat_table.send_price(bbg_sec_id, bid_price, ask_price, bid_size*1000, ask_size*1000)
        except:
            print 'Failed to send data to UAT server'
        try:
            self.pricer.prd_table.send_price(bbg_sec_id, bid_price, ask_price, bid_size*1000, ask_size*1000)
        except:
            print 'Failed to send data to PRD server'
        pass

    def basisPointShift(self,bond,oldValue,strNewValue):
        delta = float(strNewValue[:-1])
        pass

    @staticmethod
    def readInput(oldValue, strNewValue):
        '''
        Takes float and string as input, returns float
        Parser to understand +1, -0.5, +18 as +1/8 etc.
        Will also return original value if it doesn't understand input
        '''
        if strNewValue[0] == '+' or strNewValue[0] == '-':
            try:
                delta = float(strNewValue[1:])
            except:
                delta = 0
            delta_dic = {116: 0.063, 18: 0.125, 316: 0.188, 14: 0.25, 516: 0.313, 38: 0.375, 716: 0.438, 12: 0.5, 916: 0.563, 58: 0.625, 1116: 0.688, 34: 0.75, 1316: 0.813, 78: 0.875, 1516: 0.938}
            if delta in delta_dic:
                delta = delta_dic[delta]
            if strNewValue[0] == '+':
                newValue = oldValue + delta
            else:
                newValue = oldValue - delta
            newValue = round(16*newValue) / 16 #solves issues with 1/16th increments
        else:
            try:
                newValue = float(strNewValue)
            except:
                newValue = oldValue
        return newValue

    def dataSentWarning(self, row):
        for cell in ['BID', 'ASK', 'BID_S', 'ASK_S']:
            self.SetCellBackgroundColour(row, self.columnList.index(cell), wx.YELLOW)

    def onPastePrices(self, event):
        if not wx.TheClipboard.IsOpened():
            clipboard = wx.TextDataObject()
            if wx.TheClipboard.Open():
                wx.TheClipboard.GetData(clipboard)
                wx.TheClipboard.Close()
            data = clipboard.GetText()
            rowstart = self.GetGridCursorRow()
            colstart = self.GetGridCursorCol()
            for y, r in enumerate(data.splitlines()):
                # Convert c in a array of text separated by tab
                row = rowstart + y
                for x, c in enumerate(r.split('\t')):
                    self.SetCellValue(row, colstart + x, c)
                bbg_sec_id = self.GetCellValue(row,0)
                bid_price = float(self.GetCellValue(row, self.columnList.index('BID')))
                ask_price = float(self.GetCellValue(row, self.columnList.index('ASK')))
                try:
                    bid_size = int(self.GetCellValue(row, self.columnList.index('BID_S')).replace(',',''))
                    ask_size = int(self.GetCellValue(row, self.columnList.index('ASK_S')).replace(',',''))
                except:
                    bid_size = 0
                    ask_size = 0
                wx.CallAfter(self.dataSentWarning,row)
                self.pricer.uat_table.send_price(bbg_sec_id, bid_price, ask_price, bid_size*1000, ask_size*1000)
                self.pricer.prd_table.send_price(bbg_sec_id, bid_price, ask_price, bid_size*1000, ask_size*1000)

    def showPopUpMenu(self, event, fromWindowsMenu=False):
        """
        Create and display a popup menu on right-click event. Function is called by __init__() when user 
        right clicks on a grid.
        """
        menu = wx.Menu()
        if not fromWindowsMenu:
            self.clickedBond = self.GetCellValue(event.GetRow(), self.columnList.index('BOND'))
        try:
            self.clickedISIN = self.bdm.df.at[self.clickedBond, 'ISIN']
        except:
            self.clickedISIN = ''
        showAllqItem = wx.MenuItem(menu, self.showAllqID, "ALLQ")
        menu.AppendItem(showAllqItem)
        showTradeHistoryItem = wx.MenuItem(menu, self.showTradeHistoryID, "Trade history")
        menu.AppendItem(showTradeHistoryItem)
        showDESItem = wx.MenuItem(menu, self.showDESID, "DES")
        menu.AppendItem(showDESItem)
        showCNItem = wx.MenuItem(menu, self.showCNID, "CN")
        menu.AppendItem(showCNItem)
        showGPItem = wx.MenuItem(menu, self.showGPID, "GP")
        menu.AppendItem(showGPItem)
        menu.AppendSeparator()
        buyRegsItem = wx.MenuItem(menu, self.buyRegsID, "Buy REGS")
        menu.AppendItem(buyRegsItem)
        sellRegsItem = wx.MenuItem(menu, self.sellRegsID, "Sell REGS")
        menu.AppendItem(sellRegsItem)
        buy144AItem = wx.MenuItem(menu, self.buy144AID, "Buy 144A")
        menu.AppendItem(buy144AItem)
        sell144AItem = wx.MenuItem(menu, self.sell144AID, "Sell 144A")
        menu.AppendItem(sell144AItem)
        menu.AppendSeparator()
        copyLineItem = wx.MenuItem(menu, self.copyLineID, "Copy line")
        menu.AppendItem(copyLineItem)
        copyISINItem = wx.MenuItem(menu, self.copyISINID, "Copy ISIN")
        menu.AppendItem(copyISINItem)
        pastePricesItem = wx.MenuItem(menu, self.pastePricesID, "Paste prices")
        menu.AppendItem(pastePricesItem)
        self.PopupMenu(menu)
        menu.Destroy()

    def showTradeHistory(self, event):
        """
        Shows the TradeHistory.
        """
        self.bdm.mainframe.onBondQuerySub(self.clickedBond)
        wx.CallAfter(self.bdm.mainframe.Raise)
        pass

    def copyLine(self, event):
        """
        Copies the selected line to the clipboard.
        """
        self.bdm.df.loc[self.clickedBond].to_clipboard()

    def copyISIN(self, event):
        """Copies the ISIN of the selected bond to the clipboard.
        """
        if wx.TheClipboard.Open():
            wx.TheClipboard.SetData(wx.TextDataObject(self.clickedISIN))
            wx.TheClipboard.Close()

    def showDES(self, event):
        self.bbgScreenSendKeys(self.clickedISIN, 'DES')

    def showCN(self, event):
        self.bbgScreenSendKeys(self.clickedISIN, 'CN')

    def showGP(self, event):
        self.bbgScreenSendKeys(self.clickedISIN, 'GP')

    def showALLQ(self, event):
        self.bbgScreenSendKeys(self.clickedISIN, 'ALLQ')

    def buyRegs(self, event):
        self.bbgScreenSendKeys(self.clickedISIN, 'B')

    def sellRegs(self, event):
        self.bbgScreenSendKeys(self.clickedISIN, 'S')

    def buy144A(self, event):
        self.bbgScreenSendKeys(bonds.loc[regsToBondName[self.clickedISIN],'144A'], 'B')

    def sell144A(self, event):
        self.bbgScreenSendKeys(bonds.loc[regsToBondName[self.clickedISIN],'144A'], 'S')

    def bbgScreenSendKeys(self, isin, strCommand):
        """Sends command to Bloomberg.
        """
        shell = win32com.client.Dispatch("WScript.Shell")
        shell.AppActivate('1-BLOOMBERG')
        try:
            shell.SendKeys(isin + '{F3}' + strCommand + '{ENTER}')
        except:
            print 'Failed to send command to Bloomberg'

    def updateOneBenchmark(self, bond):
        """Checks if bond behchmarked or benchmarker
        """
        for b, bc in self.bondToBenchmark.iteritems():
            if bond == bc or bond == b:
                self.singleBenchmarkUpdate(b)

    def updateBenchmarks(self):
        """Called by the bond data model on first pass
        """
        for bond in self.bondsWithBenchmark:
            self.singleBenchmarkUpdate(bond)

    def singleBenchmarkUpdate(self, bond):
        """Updates single benchmark.
        """
        i = self.bondList.index(bond)
        j = self.columnList.index('BENCHMARK')
        try:
            #bench = self.tab[self.tab['Bonds'] == bond]['Benchmarks'].iloc[0]
            bench = self.bondToBenchmark[bond]
            value = self.bdm.df.at[bond, 'ZB'] - self.bdm.df.at[bench, 'ZB']
            self.SetCellBackgroundColour(i, j, wx.RED)
            self.SetCellValue(i, j, '{:,.0f}'.format(value) + ' vs ' + bench)
        except:
            self.SetCellValue(i, j, 'FAIL')
        if i % 2:
            wx.CallLater(1000, self.SetCellBackgroundColour, i, j, self.oddLineColour)
        else:
            wx.CallLater(1000, self.SetCellBackgroundColour, i, j, wx.WHITE)
        wx.CallLater(1100, self.ForceRefresh)

    def updatePositions(self, message=None):
        """Holding function that listens to the POSITION_UPDATE event and calls updateAllPositions() after
        the parent thread dies.
        """
        wx.CallAfter(self.updateAllPositions, message)

    def updateAllPositions(self, message):
        """Updates all the position. Function is called by updatePositions().
        No need for sales logic here as they only see SOD positions.
        """
        positions = message.data
        j = self.columnList.index('POSITION')
        for (i, bond) in enumerate(self.bondList):
            if bond in self.bdm.df.index and bond in positions.index:
                value = '{:,.0f}'.format(positions.at[bond, 'Qty'])
                if value != self.GetCellValue(i, j):
                    self.SetCellBackgroundColour(i, j, wx.RED)
                    self.SetCellValue(i, j, value)
                    if i % 2:
                        wx.CallLater(1000, self.SetCellBackgroundColour, i, j, self.oddLineColour)
                    else:
                        wx.CallLater(1000, self.SetCellBackgroundColour, i, j, wx.WHITE)
                    wx.CallLater(1100, self.ForceRefresh)

    def updateLine(self, message=None):
        """Holding function that listens to the BOND_PRICE_UPDATE event and calls updateLineAction() after 
        the parent thread dies.
        """
        wx.CallAfter(self.updateLineAction, message)

    def updateLineAction(self, message):
        """Updates each line. Function is called by updateLine().
        """
        series = message.data
        bond = series.name
        if bond in self.bondList:
            i = self.bondList.index(bond)
            # print self.columnList
            for col in self.columnList:
                j = self.columnList.index(col)
                value = self.createField(series, col)
                if value != 'N/A':
                    # print str(i)+'-'+str(j)
                    self.SetCellBackgroundColour(i, j, wx.RED)
                    self.SetCellValue(i, j, value)
            wx.CallLater(1000, self.resetLineColor, i)
            self.ForceRefresh() #Note, this line should be outside the for loop! Otherwise screen will refresh for every cell, which will crash the program!
        self.updateOneBenchmark(bond)
        pass

    def resetLineColor(self, i):
        for col in self.columnList:
            j = self.columnList.index(col)
            if i % 2:
                self.SetCellBackgroundColour(i, j, self.oddLineColour)
            else:
                self.SetCellBackgroundColour(i, j, wx.WHITE)
        self.ForceRefresh() #Note, this line should be outside the for loop! Otherwise screen will refresh for every cell, which will crash the program!
        pass

    def createField(self, data, fld):
        """Creates the fields to be displayed.
        """
        if fld in ['BID','ASK','MID']:
            return '{:,.3f}'.format(data[fld])
        elif fld == 'CLICK TO SEND':
            return 'SEND'
        elif fld == 'YIELD':
            return '{:,.2f}'.format(data['YLDB']) + ' / ' + '{:,.2f}'.format(data['YLDA'])
        elif fld == 'Z-SPREAD':
            return '{:,.0f}'.format(data['ZB']) + ' / ' + '{:,.0f}'.format(data['ZA'])
        elif fld == 'DP(1D/1W/1M)':
            return '{:,.2f}'.format(data['DP1D']) + ' / ' + '{:,.2f}'.format(data['DP1W']) + ' / ' + '{:,.2f}'.format(
                data['DP1M'])
        elif fld == 'DY(1D/1W/1M)':
            return '{:,.0f}'.format(data['DY1D']) + ' / ' + '{:,.0f}'.format(data['DY1W']) + ' / ' + '{:,.0f}'.format(
                data['DY1M'])
        elif fld == 'S / M / F':
            # print bond
            return data['SNP'] + ' / ' + data['MDY'] + ' / ' + data['FTC']
        elif fld == 'DZ(1D/1W/1M)':
           return '{:,.0f}'.format(data['DISP1D']) + ' / ' + '{:,.0f}'.format(data['DISP1W']) + ' / ' + '{:,.0f}'.format(
               data['DISP1M']) 
        elif fld == 'RSI14':
           return '{:,.0f}'.format(data['RSI14'])
        elif fld in ['BID_S','ASK_S']:
            return '{:,.0f}'.format(data[fld+'IZE']/1000)
        else:
            return 'N/A'

    def updateBGNPrices(self, message=None):
        wx.CallAfter(self.updateBGNPricesAction)

    def updateBGNPricesAction(self):
        j = self.columnList.index('BGN_M')
        for (i, bond) in enumerate(self.bondList):
            if bond in self.bdm.df.index:
                self.SetCellValue(i, j, '{:,.3f}'.format(self.bdm.df.at[bond, 'BGN_MID']))


class PricerWindow(wx.Frame):
    '''
    Class to create the Pricer Window (wx.Frame)

    Attributes:
        self.bdm : Class instance of BondDataModel
        self.panel : wx.Panel object 
        self.mainfram : FlowTradingGUI > MainForm class instance 
        self.noteboook : wx.Notebook object 

    Methods:
    __init__()
    onClose() : Terminates all data streams from bloomberg
    lastUpdateString() : Sets the value for last Front Data update
    onRestartBloombergConnection() : Refreshes front data
    onRefreshFrontData() : Refreshes front data
    updatePositions() : Sets the value of lastUpdateTime to self.lastUpdateString()
    onRestartBloombergConnection() : Restarts Bloomberg Connection by calling the reOpenConnection method from the BondDataModel class. 
    onRefreshSwapRates() : Refreshes the swaprate by calling refreshSwapRates (Class method of BondDataModel)
    lastSwapRefreshTime() : Calls the lastRefreshTime attribute of SwapHistory.SwapHistory to and print the time when the swap was last downlaoded from bloomberg.
    updateTime(): Function to update time whenever there's a BOND_PRICE_UPDATE event.

    ---------------------
    Back to PricingGrid
    Back to RunsGrid
    ---------------------   
    '''
    def __init__(self, mainframe=None):
        '''
        Keyword arguments:
        mainframe : FLowTradingGUI > MainForm class instance (set to None by default)
        '''

        self.mainframe = mainframe
        self.bdm = BondDataModel(self, mainframe)
        self.gridList = []

        pub.subscribe(self.updateTime, "BOND_PRICE_UPDATE")
        pub.subscribe(self.updatePositions, "POSITION_UPDATE")
        pub.subscribe(self.updateBGNPrices, "BGN_PRICE_UPDATE")

        wx.Frame.__init__(self, None, wx.ID_ANY, "Eurobond pricer", size=(1280, 800))
        favicon = wx.Icon(APPPATH+'keyboard.ico', wx.BITMAP_TYPE_ICO, 32,32)
        wx.Frame.SetIcon(self, favicon)

        self.statusbar = self.CreateStatusBar()
        self.statusbar.SetFieldsCount(4) 
        self.statusbar.SetStatusWidths([-1, -1, -1, -3])
        self.statusbar.SetStatusText('Last Front update: ' + self.lastUpdateString(),0)
        self.statusbar.SetStatusText('Last rates update: ' + datetime.datetime.now().strftime('%H:%M'),1)
        self.statusbar.SetStatusText('Last Bloomberg update: ' + datetime.datetime.now().strftime('%H:%M'),2)
        self.statusbar.SetStatusText('Sum:',3)

        self.Bind(wx.EVT_CLOSE, self.onClose)
        
        self.panel = wx.Panel(self) # main panel on the frame
        notebookPanel = wx.Panel(self.panel) # the notebook sits on the main panel
        self.notebook = wx.Notebook(notebookPanel)

        if mainframe is None or mainframe.isTrader:
            self.uat_table = inforalgo.SQLTable(inforalgo.UAT_SERVER_CONNECTION_STRING)
            self.prd_table = inforalgo.SQLTable(inforalgo.PRD_SERVER_CONNECTION_STRING)
            self.tabInforalgoControlPanel = inforalgopanel.InforalgoControlPanel(parent = self.notebook, uat_table = self.uat_table, prd_table = self.prd_table, bdm = self.bdm)
            self.notebook.AddPage(self.tabInforalgoControlPanel, 'Inforalgo')
            self.tabRuns = wx.Panel(parent=self.notebook)
            self.notebook.AddPage(self.tabRuns, 'Runs')

        defaultColumnList = ['ISIN', 'BOND','BID', 'ASK', 'BID_S','ASK_S', 'BGN_M', 'POSITION', 'YIELD', 'Z-SPREAD', 'DP(1D/1W/1M)','DZ(1D/1W/1M)',
                             'BENCHMARK', 'RSI14', 'ACCRUED', 'D2CPN', 'S / M / F', 'COUPON', 'MATURITY', 'SIZE']#removed columns: 'DY(1D/1W/1M)' POS AFTER RSI14
        try:
            grid_labels = list(tabList[GetUserName()][tabList[GetUserName()].notnull()])
            columnList = list(columnListByTrader[GetUserName()][columnListByTrader[GetUserName()].notnull()])
        except:
            grid_labels = list(tabList['other'][tabList['other'].notnull()])
            columnList = defaultColumnList
        ####DEBUG MODE######
        #grid_labels = ['Africa', 'IRHedges']# used for testing
        ####END DEBUG MODE######
        for label in grid_labels:#
            csv = pandas.read_csv(DEFPATH+label+'Tab.csv')
            csv['Bonds'].fillna('', inplace=True)
            tab = wx.Panel(parent=self.notebook)
            grid = PricingGrid(tab, csv, columnList, self.bdm, self)
            self.gridList.append(grid)
            self.notebook.AddPage(tab, label)
            sizer = wx.BoxSizer()
            sizer.Add(grid, proportion=1, flag=wx.EXPAND)
            tab.SetSizerAndFit(sizer)

        self.bdm.reduceUniverse()
        number_of_bonds = len(self.bdm.df['ISIN'])

        topframe = self if mainframe is None else mainframe
        #old_style = mainframe.GetWindowStyle()
        #mainframe.SetWindowStyle(old_style | wx.STAY_ON_TOP)
        busyDlg = wx.BusyInfo('Fetching price history from Bloomberg for '+str(number_of_bonds)+' bonds...', parent=topframe)
        self.bdm.fillHistoricalPricesAndRating()
        self.bdm.fillPositions()
        busyDlg = None
        #topframe.SetWindowStyle(old_style)

        if mainframe is None or mainframe.isTrader:
            gridRuns = RunsGrid(self.tabRuns, bondRuns, self.bdm, self)
            sizerRuns = wx.BoxSizer(wx.VERTICAL)
            btn = wx.Button(self.tabRuns, label="Reload run definitions")
            btn.Bind(wx.EVT_BUTTON, gridRuns.onReloadRunDefinitions)
            sizerRuns.Add(btn, 0.1, wx.EXPAND, 2)
            sizerRuns.Add(gridRuns, proportion=1, flag=wx.EXPAND)
            self.tabRuns.SetSizerAndFit(sizerRuns)

        # ##MAIN WINDOW LAYOUT
        sizer = wx.BoxSizer(wx.VERTICAL)
        buttonsPanel = wx.Panel(self.panel)
        sizer.Add(buttonsPanel, 0.25, wx.EXPAND, 10)
        buttonPanelSizer = wx.GridSizer(1,7,0,0)
        #Create buttons
        self.frontButton = wx.Button(buttonsPanel, label='Refresh trades')
        self.frontButton.Bind(wx.EVT_BUTTON, self.onRefreshFrontData)
        #self.lastUpdateTime = wx.TextCtrl(buttonsPanel, -1, self.lastUpdateString())
        ratesButton = wx.Button(buttonsPanel, label='Refresh rates')
        ratesButton.Bind(wx.EVT_BUTTON, self.onRefreshSwapRates)
        #self.ratesUpdateTime = wx.TextCtrl(buttonsPanel, -1, 'Starting...')
        bloomButton = wx.Button(buttonsPanel, label="Restart Bbrg link")
        bloomButton.Bind(wx.EVT_BUTTON, self.onRestartBloombergConnection)
        axeButton = wx.Button(buttonsPanel, label="Axe sheet")
        axeButton.Bind(wx.EVT_BUTTON, self.onAxeSheet)
        #self.bloomUpdateTime = wx.TextCtrl(buttonsPanel, -1, 'Starting...')
        editTabButton = wx.Button(buttonsPanel, label='Edit tab')
        editTabButton.Bind(wx.EVT_BUTTON, self.onEditTab)
        tipButton = wx.Button(buttonsPanel, label='Tips')
        tipButton.Bind(wx.EVT_BUTTON, self.onTips)
        aboutButton = wx.Button(buttonsPanel, label='Pricer guide')
        aboutButton.Bind(wx.EVT_BUTTON, self.onAbout)
        if mainframe is None:
            self.frontButton.Enable(False)
        else:
            if not mainframe.isTrader:
                self.frontButton.Enable(False)
        buttonPanelSizer.AddMany([
            (self.frontButton,1,wx.EXPAND,2),
            (ratesButton,1,wx.EXPAND,2),
            (bloomButton,1,wx.EXPAND,2),
            (axeButton,1,wx.EXPAND,2),
            (editTabButton,1,wx.EXPAND,2),
            (tipButton,1,wx.EXPAND,2),
            (aboutButton,1,wx.EXPAND,2)#,
            #(self.lastUpdateTime,1,wx.EXPAND,2),
            #(self.ratesUpdateTime,1,wx.EXPAND,2),
            #(self.bloomUpdateTime,1,wx.EXPAND,2)
            ])

        buttonsPanel.SetSizer(buttonPanelSizer)
            
        sizer.Add(notebookPanel, 1, wx.EXPAND, 0)
        notebookPanelSizer = wx.BoxSizer(wx.VERTICAL)
        notebookPanelSizer.Add(self.notebook, 1, wx.EXPAND)
        notebookPanel.SetSizer(notebookPanelSizer)
        self.panel.SetSizer(sizer)
        self.Layout()
        wx.CallAfter(self.Refresh)

        ################START UPDATES###############
        for grid in self.gridList:
            wx.CallAfter(grid.initialPaint)
        
        priorityBondList = []
        busyDlg = wx.BusyInfo('Downloading analytics for ' + str(number_of_bonds) + ' bonds...', parent=topframe)
        self.bdm.firstPass(priorityBondList)
        # self.ratesUpdateTime.SetValue(self.lastSwapRefreshTime())
        busyDlg = None 
        self.bdm.startUpdates()
        pub.sendMessage('BDM_READY', message = MessageContainer(self.bdm))
        ############################################

    def onAbout(self, event):
        TextDisplayWindow("Pricer Guide",'documentation//PricerGuide.txt')

    def onTips(self,event):
        TextDisplayWindow("Usage tips",'documentation//PricerInputTips.txt')

    def onEditTab(self,event):
        tabName = self.notebook.GetPageText(self.notebook.GetSelection())
        if tabName != 'Inforalgo':
            if tabName == 'Runs':
                filename = DEFPATH + 'runs.csv'
            else:
                filename = DEFPATH + tabName + 'Tab.csv'
            Popen(filename, shell = True)

    def onClose(self, event):
        '''
        Terminates all data streams from Bloomberg
        '''
        try:
            self.bdm.blptsAnalytics.closeSession()
            self.bdm.blptsAnalytics = None
            self.bdm.bbgstreamBID.closeSubscription()
            self.bdm.bbgstreamBID = None
            self.bdm.streamWatcherBID = None
            self.bdm.streamWatcherAnalytics = None
        except:
            pass
        self.bdm = None
        self.Destroy()

    def lastUpdateString(self):
        '''
        Sets the value for last Front Data update
        '''
        try:
            if self.mainframe.th.df['Date'].iloc[-1] != datetime.datetime.today().strftime('%d/%m/%y'):
                return self.mainframe.th.df['Date'].iloc[-1] + '.'
            else:
                return 'Last updated today at ' + datetime.datetime.now().strftime('%H:%M') + '.'
        except:
            return 'N/A'

    def onRefreshFrontData(self, event):
        '''
        Refreshes front data
        '''
        self.frontButton.Disable()
        #self.lastUpdateTime.SetValue('Requested data update, please wait...')
        self.statusbar.SetStatusText('Pulling trade data...',0)
        self.mainframe.onTodayTrades(event)

    def updatePositions(self, message=None):
        """Sets the value of lastUpdateTime to self.lastUpdateString()
        """
        #self.lastUpdateTime.SetValue(self.lastUpdateString())
        self.statusbar.SetStatusText('Last Front update: ' + datetime.datetime.now().strftime('%H:%M'),0)
        self.frontButton.Enable()

    def onRefreshSwapRates(self, event):
        '''
        Refreshes the swap rates by calling refreshSwapRates (Class method of BondDataModel)
        '''
        #busyDlg = wx.BusyInfo('Refreshing rates...')
        self.statusbar.SetStatusText('Refreshing swap rates...',2)
        self.bdm.refreshSwapRates()
        #self.ratesUpdateTime.SetValue(self.lastSwapRefreshTime())
        self.statusbar.SetStatusText('Last rates update: ' + datetime.datetime.now().strftime('%H:%M'),2)
        #busyDlg = None
        pass

    def onRestartBloombergConnection(self, event):
        '''
        Restarts Bloomberg Connection by calling the reOpenConnection method from the BondDataModel class. 
        '''
        #busyDlg = wx.BusyInfo('Restarting Bloomberg Connection...')
        self.statusbar.SetStatusText('Restarting Bloomberg link...',2)
        self.bdm.reOpenConnection()
        #self.bloomUpdateTime.SetValue(self.lastUpdateString())
        self.statusbar.SetStatusText('Last Bloomberg restart: ' + datetime.datetime.now().strftime('%H:%M'),2)
        #busyDlg = None 
        pass

    def onAxeSheet(self, event):
        tabName = self.notebook.GetPageText(self.notebook.GetSelection())
        csv = pandas.read_csv(DEFPATH+tabName+'Tab.csv')
        csv['Bonds'].fillna('', inplace=True)
        AxeGrid('Axe list: ' + tabName,self.bdm, csv['Bonds'])
        pass

    def lastSwapRefreshTime(self):
        '''
        Calls the lastRefreshTime attribute of SwapHistory.SwapHistory to print the time when the swap was last 
        downlaoded from Bloomberg.
        '''
        return 'Last refreshed at: ' + self.bdm.USDswapRate.lastRefreshTime.strftime('%H:%M:%S') + '.' 

    def updateTime(self, message=None):
        """Function to update time whenever there's a BOND_PRICE_UPDATE event.
        """
        #self.statusbar.SetStatusText('Last Bloomberg update: ' + datetime.datetime.now().strftime('%H:%M'),2)
        pass

    def updateBGNPrices(self, message=None):
        self.statusbar.SetStatusText('Last BGN update: ' + datetime.datetime.now().strftime('%H:%M'),2)
        pass


if __name__ == "__main__":
    app = wx.App()
    frame = PricerWindow().Show()
    app.MainLoop()
