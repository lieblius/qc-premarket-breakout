import pandas as pd
from io import StringIO

class PremarketBreakout(QCAlgorithm):
    
    gapper_data = None
    premarket_high = {}
    
    def Initialize(self):
        self.gapper_data = pd.read_csv(StringIO(self.Download("https://raw.githubusercontent.com/lieblius/financial-data/main/gappers.csv")), index_col='Date')
        
        self.SetStartDate(2018, 1, 1)  
        self.SetEndDate(2019, 1, 10)  
        self.SetCash(100000)
        
        self.UniverseSettings.Resolution = Resolution.Minute
        
        #3. Create a scheduled event triggered at 13:30 calling the ClosePositions function
        self.Schedule.On(self.DateRules.EveryDay(), self.TimeRules.At(12, 00) , self.ClosePositions)
        
        # Selection will run on mon/tues/thurs at 00:00/06:00/12:00/18:00
        self.AddUniverseSelection(ScheduledUniverseSelectionModel(
            self.DateRules.EveryDay(),
            self.TimeRules.At(00, 00),
            self.SelectSymbols # selection function in algorithm.
        ))
        
    def OnData(self, data):
        if 9 <= self.Time.hour < 12 :
            if self.Time.hour == 9 and self.Time.minute < 30:
                return
            for security in self.Securities.keys():
                if security.Symbol not in self.premarket_high:
                    return
                if data[security].Close > self.premarket_high[security.Symbol]:
                    self.MarketOrder(security, 500)
    
    def ClosePositions(self):
        self.premarket_high = {}
        
        for ticker in self.Securities.keys():
            self.Liquidate(ticker)

    # Create selection function which returns symbol objects.
    def SelectSymbols(self, dateTime):
        symbols = []
        date = str(dateTime.date())
        if date in self.gapper_data.index:
            gappers = self.gapper_data.loc[date]
            # self.Log(date)
            gappers = self.gapper_data.loc[str(date)]
            if isinstance(gappers, pd.Series):
                symbol = gappers['Symbol']
                self.premarket_high[symbol] = gappers['Premarket High']
                # self.Log(symbol)
                symbols.append(Symbol.Create(symbol, SecurityType.Equity, Market.USA))
            else:
                for i in range(len(gappers)):
                    symbol = gappers.iloc[i]['Symbol']
                    self.premarket_high[symbol] = gappers.iloc[i]['Premarket High']
                    # self.Log(symbol)
                    symbols.append(Symbol.Create(symbol, SecurityType.Equity, Market.USA))
        return symbols
