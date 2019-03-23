from botocore.vendored import requests

class AlphaVantage:
    
    def __init__(self,key):
        self.key = key
        self.historic_data = None
        self.renamer = {
            '3. low':'low', 
            '1. open':'open', 
            '2. high':'high',
            '4. close':'close', 
            '6. volume':'volume', 
            '5. adjusted close':'adj_close', 
            '8. split coefficient':'split', 
            '7. dividend amount':'dividend'
        }
        
    def historic(self,symbol):
        self.symbol = symbol
        url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=MSFT&outputsize=full&apikey=demo"
        url = url.replace('MSFT', symbol).replace('demo',self.key)
        historic_data = requests.get(url).json()['Time Series (Daily)']
        return self.dlist(historic_data)
    
    def recent(self,symbol):
        self.symbol = symbol
        url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=MSFT&apikey=demo"
        url = url.replace('MSFT', symbol).replace('demo',self.key)
        data = requests.get(url).json()['Time Series (Daily)']
        return self.dlist(data)
    
    def dlist(self, dataset):
        return [self.obj_converter(date, data) for date, data in dataset.items()]
        
    def obj_converter(self, date, obj):
        res = dict()
        res['date'] = date
        res['symbol'] = self.symbol
        for k,v in self.renamer.items():
            res[v] = obj.get(k)
        return res
    
    
    
