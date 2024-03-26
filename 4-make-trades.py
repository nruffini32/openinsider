from utils import PaperAccount, Database, CloudStorage

# Script to make trades using staging_trades. Will delete staging trades on completion.
print("Starting script.\n")

STAGING_TABLE = "staging_trades"
BANKROLL = 1000

paper = PaperAccount()
db = Database()
sto = CloudStorage("4_make_trades")

# If staging_table is empty or doesn't exists exiting script
sto.print("Fetching data from staging_trades.\n")
data = db.get_table_data(STAGING_TABLE)
if not data:
    sto.print(f"{STAGING_TABLE} is empty. Exiting script.")
    exit()

# Looping through each trade in staging_trades
for d in data:
    ticker = d[3]
    trade_type = d[7]
    price = d[8]
    qty = d[9]
    owned = d[10]

    # If shares were given or trade_type is not a straight up purchase or sale then skip
    if price == 0 or trade_type not in ["P - Purchase", "S - Sale"]:
        continue

    # sto.print(f"{trade_type} - {ticker}")
    # sto.print(f"p:${price} q:{qty} owned:{owned}")

    # Buying some of stock depending on how much they aquired compared to how much they previously owned
    if trade_type == "P - Purchase":
        percent_of_owned = round((qty / owned), 5)
        buy_amount = BANKROLL * percent_of_owned

        sto.print(f"{trade_type} - {ticker}")
        sto.print(f"INSIDER INFO - p:${price} q:{qty} owned:{owned} my buy amount: {buy_amount}")

        # Trying dollar amount purchase - if it doesn't work then buying full shares
        paper.buy_stock(ticker, buy_amount, price)

    # Logic for selling
    elif trade_type == "S - Sale":
        # Getting my current positions and creating dictionary
        positions = paper.current_positions()
        my_tickers = {i.symbol: {
                                    "qty": i.qty,
                                    "market_value": i.market_value, 
                                    "avg_entry_price": i.avg_entry_price, 
                                    "current_price": i.current_price } 
                        for i in positions }
        # sto.print(my_tickers)

        # If I do not own ticker that is in trade then skip
        if ticker not in my_tickers:
            sto.print(f"Do not own any {ticker}.\n")
            continue

        # Get my position data of current ticker
        data = my_tickers[ticker]
        tot_value = float(data["market_value"])
        shares = data["qty"]
        sto.print(f"MY INFO FOR {ticker} - val:{tot_value} shares:{shares}")

        # Calculate the percentage of my shares I want to sell based on how much insider sold
        qty = abs(qty)
        percent_of_owned = round((qty / owned), 5)
        selling_value = round(tot_value * percent_of_owned, 2)
        sto.print(f"Insider owned {owned}. They sold {qty}. That is {percent_of_owned} of their total shares.")
        sto.print(f"I should sell {selling_value}")

        # Selling stock
        paper.sell_stock(ticker, selling_value, price)

    sto.print("")

# Deleting staging table
sto.print(f"Deleting table {STAGING_TABLE}")
db.drop_table(STAGING_TABLE)
    
# Have to run this so logging file is actually created.
sto.close_file()