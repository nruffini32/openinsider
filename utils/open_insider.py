import requests
from bs4 import BeautifulSoup
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime, timedelta, date

# X
# A	Amended filing
# D	Derivative transaction in filing (usually option exercise)
# E	Error detected in filing
# M	Multiple transactions in filing; earliest reported transaction date and weighted average transaction price

# Daily flow
#   1. Ingest all new records (already implemented)
#   2. Create purchases table
#   3. Create sales table ?
#   4. Cluster table ?
#   5. Get price now of all stocks
#       

### FOR EACH TRADE I WANT TO SEE ALL POSITIONS AND HOW MUCH MONEY THEY HAVE MADE
        # FOR EACH PERSON I WANT TO SEE HOW MUCH MONEY THEY HAVE MADE - HOW MUCH DID STOCK GO UP AFTER THEY BOUGHT IT - START THERE
        ## START AT THIS - FOR EACH TRADE - HOW MUCH STOP HAS MOVED SINCE THEN - HOW MUCH THEY MADE

############# HEHREHRHEHERH -  maybe look into cluster buys?


class InsiderScraper:
    def __init__(self) -> None:
        self.headers = {
            "user-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
        }
        self.keys = [
            "x",
            "filing_date",
            "trade_date",
            "ticker",
            "company_name",
            "insider_name",
            "title",
            "trade_type",
            "price",
            "qty",
            "owned",
            "change_of_owned",
            "value"
        ]
            
    def _scrape_openinsider(self, url):
        """
        Returns list of trades given url param.
        """
        try:
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                rows = soup.find("table", class_="tinytable").find("tbody").find_all("tr")
            else:
                raise Exception(f"{response} - Failed to fetch data from {url}")
        except:
            raise Exception(f"This url was not successful: {url}")

        # Creating a list of dictionaries - each dictionary is a trade
        trades = []
        for row in rows:
            items = row.find_all("td")
            dic = {}
            # Looping through items in row and creating dictionary
            for key, val in zip(self.keys, items[:-4]):
                dic[key] = val.text.strip()
            # Adding dictionary to list
            trades.append(dic)
        return trades

    def get_recent_trades(self, cnt=1000):
        """
        Returns most recent trades from http://openinsider.com/latest-insider-trading

        Args:
            cnt: how many trades (5000 max)
        """
        url = f"http://openinsider.com/screener?s=&o=&pl=&ph=&ll=&lh=&fd=730&fdr=&td=0&tdr=&fdlyl=&fdlyh=&daysago=&xp=1&xs=1&vl=&vh=&ocl=&och=&sic1=-1&sicl=100&sich=9999&grp=0&nfl=&nfh=&nil=&nih=&nol=&noh=&v2l=&v2h=&oc2l=&oc2h=&sortcol=0&cnt={cnt}&page=1"
        trades = self._scrape_openinsider(url)
        return trades
            
    def get_trades_for_date(self, input_date):
        """
        Returns all trades for a certain date.

        Args:
            input_date: which date you want trades from (can be datetime object or string in the format MM/DD/YYYY)
        """
        # Return empty list of input_date is a weekend
        if input_date.weekday() in [5, 6]:
            return []
        
        if isinstance(input_date, datetime):
            date_string = input_date.strftime('%m/%d/%Y')

        date_range = f"{date_string}+-+{date_string}"
        url = f'http://openinsider.com/screener?s=&o=&pl=&ph=&ll=&lh=&fd=-1&fdr={date_range}&td=0&tdr=&fdlyl=&fdlyh=&daysago=&xp=1&xs=1&vl=&vh=&ocl=&och=&sic1=-1&sicl=100&sich=9999&grp=0&nfl=&nfh=&nil=&nih=&nol=&noh=&v2l=&v2h=&oc2l=&oc2h=&sortcol=0&cnt=5000&page=1'
        
        trades = self._scrape_openinsider(url)
        return trades
    
    def get_trades_for_month(self, month, year):
        """
        Returns all trades for month year.

        Args:
            month INT: month you want trades from.
            year INT: year you want trades from
        """
        start_date = datetime(year, month, 1).strftime('%m/%d/%Y')
        end_date = (datetime(year, month, 1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        end_date = end_date.strftime('%m/%d/%Y')
        url = f'http://openinsider.com/screener?s=&o=&pl=&ph=&ll=&lh=&fd=-1&fdr={start_date}+-+{end_date}&td=0&tdr=&fdlyl=&fdlyh=&daysago=&xp=1&xs=1&vl=&vh=&ocl=&och=&sic1=-1&sicl=100&sich=9999&grp=0&nfl=&nfh=&nil=&nih=&nol=&noh=&v2l=&v2h=&oc2l=&oc2h=&sortcol=0&cnt=5000&page=1'
        trades = self._scrape_openinsider(url)

        # If trades is 5000 - means there were more than 5000 trades in a month and have to get the rest of the trades
        if len(trades) == 5000:
            # Removing all trades at the cut off
            last_date = trades[-1]["filing_date"].split(" ")[0]
            for item in trades[::-1]:
                filing_date = item['filing_date'].split(" ")[0]
                if filing_date == last_date:
                    trades.remove(item)
                else:
                    break

            # Adding trades for days that were not included - starting at last date to first date to keep list in order
            start_date = datetime(year, month, 1)
            end_date = datetime.strptime(last_date, '%Y-%m-%d')
            date_difference = end_date - start_date
            for day in range(date_difference.days + 1):
                current_date = end_date - timedelta(days=day)
                new_trades = self.get_trades_for_date(current_date)
                trades = trades + new_trades

            return trades
        else:
            return trades
        

    def get_stock_price(self, client, tickers, input_date):
        """
        Returns stock price of ticker for date.
        200 calls / min in the limit - ticker can be list of strings or just a string

        Args:
            client (alpaca.data.StockHistoricalDataClient): client from alpaca - StockHistoricalDataClient(config.ALPACA_KEY,  config.ALPACA_SECRET)
            tickers (str or List[str]): stock market tickers
            date (str): date to return info for - in format "2024-03-12")

        Returns:
            dictionary of tickers and info
                {'NTRA': [{   'close': 89.06,
                'high': 93.48,
                'low': 87.62,
                'open': 92.14,
                'symbol': 'NTRA',
                'timestamp': datetime.datetime(2024, 3, 8, 5, 0, tzinfo=TzInfo(UTC)),
                'trade_count': 18570.0,
                'volume': 1067978.0,
                'vwap': 89.446311}]}
        """

        # symbol_or_symbols (Union[str, List[str]]): The ticker identifier or list of ticker identifiers.
        # timeframe (TimeFrame): The period over which the bars should be aggregated. (i.e. 5 Min bars, 1 Day bars)
        # start (Optional[datetime]): The beginning of the time interval for desired data. Timezone naive inputs assumed to be in UTC.
        # end (Optional[datetime]): The end of the time interval for desired data. Defaults to now. Timezone naive inputs assumed to be in UTC.
        # limit (Optional[int]): Upper limit of number of data points to return. Defaults to None.
        # adjustment (Optional[Adjustment]): The type of corporate action data normalization.
        # feed (Optional[DataFeed]): The stock data feed to retrieve from.
        # sort (Optional[Sort]): The chronological order of response based on the timestamp. Defaults to ASC.
        
        # Convert string to datetime object
        if isinstance(input_date, str):
            input_date = datetime.strptime(input_date, "%Y-%m-%d")
        next_day = input_date + timedelta(days=1)
        next_day_string = next_day.strftime("%Y-%m-%d")

        # Removing end param if day is today
        today = date.today().strftime("%Y-%m-%d")
        if input_date == today:
            params = StockBarsRequest(
                            symbol_or_symbols=tickers,
                            timeframe=TimeFrame.Day,
                            start=input_date,      
                    )
        else:
            params = StockBarsRequest(
                            symbol_or_symbols=tickers,
                            timeframe=TimeFrame.Day,
                            start=input_date,
                            end=next_day_string
                    )
            
        # Making request
        try:
            resp = client.get_stock_bars(params)
            return resp.data
        except Exception as e:
            raise Exception(f"Error with get_stock_price: {e}")