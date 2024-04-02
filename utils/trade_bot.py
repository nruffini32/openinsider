from .credentials import config
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from .database import Database

class PaperAccount():
    def __init__(self) -> None:
        self.client = TradingClient(config.ALPACA_KEY,  config.ALPACA_SECRET, paper=True)
        self.db = Database()

    def get_client(self):
        return self.client
    
    def buy_stock(self, ticker, dollar_amount, price_of_stock):
        """
        Buying x amount of a stock. If fractional purchase doesn't work then trying full share purchase

        Args:
            ticker str: stock market ticker
            dollar_amount int: amount you want to sell
        """
        trade_type = "P - Purchase"
        try:
            dollar_amount = int(dollar_amount)
            # preparing orders
            market_order_data = MarketOrderRequest(
                                symbol=ticker,
                                notional=dollar_amount,
                                side=OrderSide.BUY,
                                time_in_force=TimeInForce.DAY
                                )
            # Place order
            market_order = self.client.submit_order(
                            order_data=market_order_data
                        )
        
            print(f"Bought ${dollar_amount} of {ticker}")
            print(f"Inserting ticker:{ticker} type:${trade_type.split(" ")[-1]} dollar/share?:{"D"} amount:{dollar_amount}")
            resp = self.db.insert_order(market_order.id, ticker, trade_type, "D", dollar_amount)
            resp.result() ## Wait for job to finish
            return market_order
        except:
            try:
                shares_to_buy = round(dollar_amount / price_of_stock)
                # preparing orders
                market_order_data = MarketOrderRequest(
                                    symbol=ticker,
                                    qty=shares_to_buy,
                                    side=OrderSide.BUY,
                                    time_in_force=TimeInForce.DAY
                                    )
                
                # Place order
                market_order = self.client.submit_order(
                                order_data=market_order_data
                            )
                
                print(f"Bought {shares_to_buy} shares of {ticker}. Total val - {shares_to_buy * price_of_stock}")
                print(f"Inserting ticker:{ticker} type:${trade_type.split(" ")[-1]} dollar/share?:{"S"} amount:{shares_to_buy}")
                resp = self.db.insert_order(market_order.id, ticker, trade_type, "S", shares_to_buy)
                resp.result() ## Wait for job to finish

                return market_order
            except Exception as e:
                print(f"Couldn't buy any {ticker}. error - {e}")


    def sell_stock(self, ticker, dollar_amount, price_of_stock):
        """
        Selling x amount of a stock.

        Args:
            ticker str: stock market ticker
            dollar_amount int: amount you want to sell
        """
        trade_type = "S - Sale"
        try:
            dollar_amount = int(dollar_amount)
            # preparing orders
            market_order_data = MarketOrderRequest(
                                symbol=ticker,
                                notional=dollar_amount,
                                side=OrderSide.SELL,
                                time_in_force=TimeInForce.DAY
                                )
            # Place order
            market_order = self.client.submit_order(
                            order_data=market_order_data
                        )
            
            print(f"Sold ${dollar_amount} of {ticker}")
            print(f"Inserting ticker:{ticker} type:${trade_type.split(" ")[-1]} dollar/share?:{"D"} amount:{dollar_amount}")
            resp = self.db.insert_order(market_order.id, ticker, trade_type, "D", dollar_amount)
            resp.result() ## Wait for job to finish

            return market_order
        except:
            try:
                shares_to_buy = round(dollar_amount / price_of_stock)
                # preparing orders
                market_order_data = MarketOrderRequest(
                                    symbol=ticker,
                                    qty=shares_to_buy,
                                    side=OrderSide.SELL,
                                    time_in_force=TimeInForce.DAY
                                    )
                
                # Place order
                market_order = self.client.submit_order(
                                order_data=market_order_data
                            )
                
                print(f"Sold {shares_to_buy} shares of {ticker}. Total val - {shares_to_buy * price_of_stock}")
                print(f"Inserting ticker:{ticker} type:${trade_type.split(" ")[-1]} dollar/share?:{"S"} amount:{shares_to_buy}")
                resp = self.db.insert_order(market_order.id, ticker, trade_type, "S", shares_to_buy)
                resp.result() ## Wait for job to finish 
                
                return market_order
            except Exception as e:
                print(f"Couldn't sell any {ticker}. error - {e}")

    def current_positions(self):
        """
        Returns all current positions.
        """
        positions = self.client.get_all_positions()
        return positions

    def wipe_account(self):
        """
        Closes all positions and cancels all orders.
        """
        # closes all position AND also cancels all open orders
        self.client.close_all_positions(cancel_orders=True)
        print("Wiping paper account")

    


if __name__ == "__main__":
    paper = PaperAccount()
    client = paper.get_client()

    # positions = paper.get_positions()
    # print(positions)
    paper.wipe_account()




