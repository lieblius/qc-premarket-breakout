import pandas as pd
from io import StringIO

class PremarketBreakout(QCAlgorithm):
    
    dollar_risk_per_trade = 200
    target_percent = 0
    gapper_data = None
    limit_order = {}
    stop_order = {}
    premarket_high = {}
    traded_today = set()
    
    def Initialize(self):
        self.gapper_data = pd.read_csv(StringIO(self.Download("https://raw.githubusercontent.com/lieblius/financial-data/main/gappers.csv")), index_col='Date')
        
        self.target_percent = float(self.GetParameter("target-percent"))
        
        self.SetStartDate(2021, 1, 1)  
        self.SetEndDate(2021, 5, 4)  
        self.SetCash(25000)
        
        self.SetExecution(ImmediateExecutionModel())
        self.UniverseSettings.Resolution = Resolution.Second
        self.SetSecurityInitializer(self.CustomSecurityInitializer)
        
        #3. Create a scheduled event triggered at 13:30 calling the ClosePositions function
        self.Schedule.On(self.DateRules.EveryDay(), self.TimeRules.At(12, 00) , self.ClosePositions)
        
        # Selection will run on mon/tues/thurs at 00:00/06:00/12:00/18:00
        self.AddUniverseSelection(ScheduledUniverseSelectionModel(
            self.DateRules.EveryDay(),
            self.TimeRules.At(00, 00),
            self.SelectSymbols # selection function in algorithm.
        ))
        
    def OnOrderEvent(self, order_event):
        if order_event.Symbol in self.stop_order and self.stop_order[order_event.Symbol].Status == OrderStatus.Filled:
            self.limit_order[order_event.Symbol].Cancel("Stop filled.")
        elif order_event.Symbol in self.limit_order and self.limit_order[order_event.Symbol].Status == OrderStatus.Filled:
            self.stop_order[order_event.Symbol].Cancel("Limit filled.")

    def CustomSecurityInitializer(self, security):
        security.SetDataNormalizationMode(DataNormalizationMode.Raw)
        security.SetFeeModel(ConstantFeeModel(0))
        security.SetSlippageModel(ConstantSlippageModel(0))
        security.SetFillModel(ImmediateFillModel())
        
    def OnData(self, data):
        if 9 <= self.Time.hour < 12 :
            if self.Time.hour == 9 and self.Time.minute < 30:
                return
            for security in self.ActiveSecurities:
                equity = security.Value
                symbol = equity.Symbol
                symbol_string = symbol.Value
                if not equity.HasData:
                    continue
                if equity.Invested:
                    continue 
                if symbol_string not in self.premarket_high:
                    continue
                if symbol_string in self.traded_today:
                    continue
                # if len(self.traded_today) >= 5:
                #     continue
                
                current_price = data[symbol].Close
                
                if current_price > self.premarket_high[symbol_string]:
                    # self.Log("Buying PMH break")
                    # self.Log(dir(security.Value.Symbol))
                    # self.Log(symbol_string)
                    # self.Log(data[symbol].Close)
                    # self.Log(self.premarket_high[symbol_string])
                    quantity = int((self.dollar_risk_per_trade/self.target_percent)/current_price)
                    
                    self.traded_today.add(symbol_string)
                    self.MarketOrder(symbol, quantity)
                    self.limit_order[symbol] = self.LimitOrder(symbol, -quantity, round(self.premarket_high[symbol_string] * (1 + self.target_percent), 2))
                    self.stop_order[symbol] = self.StopMarketOrder(symbol, -quantity, round(self.premarket_high[symbol_string] * (1 - self.target_percent), 2))


    
    def ClosePositions(self):
        self.premarket_high = {}
        self.traded_today = set()
        self.Liquidate()
        self.Transactions.CancelOpenOrders()

    # Create selection function which returns symbol objects.
    def SelectSymbols(self, dateTime):
        min_gap = float(self.GetParameter("min-gap-pct"))
        min_pmh_price = float(self.GetParameter("min-premarkethigh-price"))
        max_daily_trades = int(self.GetParameter("max-daily-trades"))
        symbols = []
        date = str(dateTime.date())
        if date in self.gapper_data.index:
            gappers = self.gapper_data.loc[date]
            # self.Log(date)
            gappers = self.gapper_data.loc[str(date)]
            if isinstance(gappers, pd.Series):
                symbol = gappers['Symbol']
                if gappers['GAP%'] >= min_gap and gappers['Premarket High'] >= min_pmh_price:# \
                # and gappers['Outstanding Shares'] <= 20000000 and gappers['Market Cap'] <= 20000000:
                    self.premarket_high[symbol] = gappers['Premarket High']
                    symbols.append(Symbol.Create(symbol, SecurityType.Equity, Market.USA))
            elif len(gappers) <= max_daily_trades:
                for i in range(len(gappers)):
                    symbol = gappers.iloc[i]['Symbol']
                    if gappers.iloc[i]['GAP%'] >= min_gap and gappers.iloc[i]['Premarket High'] >= min_pmh_price:# \
                    # and gappers.iloc[i]['Outstanding Shares'] <= 20000000 and gappers.iloc[i]['Market Cap'] <= 20000000:
                            self.premarket_high[symbol] = gappers.iloc[i]['Premarket High']
                            symbols.append(Symbol.Create(symbol, SecurityType.Equity, Market.USA))
            else:
                top_list = []
                for i in range(len(gappers)):
                    symbol = gappers.iloc[i]['Symbol']
                    if gappers.iloc[i]['GAP%'] >= min_gap and gappers.iloc[i]['Premarket High'] >= min_pmh_price:# \
                    # and gappers.iloc[i]['Outstanding Shares'] <= 20000000 and gappers.iloc[i]['Market Cap'] <= 20000000:
                        top_list.append(gappers.iloc[i])
                top_list = sorted(top_list, key=lambda g: g['GAP%'], reverse=True)[:max_daily_trades]
                for gapper in top_list:
                    symbol = gapper['Symbol']
                    self.premarket_high[symbol] = gapper['Premarket High']
                    symbols.append(Symbol.Create(symbol, SecurityType.Equity, Market.USA))
        return symbols
