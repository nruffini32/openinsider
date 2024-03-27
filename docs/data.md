## Schemas
**trades_bronze**: Raw trades from openinsider.com

<img width="175" alt="image" src="https://github.com/nruffini32/openinsider/assets/71286321/80a36d94-5790-4cfb-aac7-ee91f47527b7">
<br>
<br>

**staging_trades**: Recently processed trades - used in downstream scripts. Deleted at the end of pipeline execution
- schema is same as trades_bronze

**trades**: Subset of trades_bronze with applied transformations

<img width="175" alt="image" src="https://github.com/nruffini32/openinsider/assets/71286321/8dce0631-0fa5-4d76-b6cf-f1d94d9e811e">
<br>
<br>

**ticker_data**: Stock market data for all stocks at all dates they were traded at

<img width="175" alt="image" src="https://github.com/nruffini32/openinsider/assets/71286321/0f2bb822-d005-4bf4-b802-ece8dfc7c66b">
<br>
<br>

**recent_ticker_data**: Current stock market data for all stocks
- schema is same as ticker_data

**my_orders**: All order that have been placed in Alpaca paper trading account

<img width="175" alt="image" src="https://github.com/nruffini32/openinsider/assets/71286321/c0078600-6eae-4aac-83a7-d95560055c64">
<br>
<br>

#### Views
**trades_ticker_data**: Joining trades and ticker_data to get ticker data for each trade

<img width="175" alt="image" src="https://github.com/nruffini32/openinsider/assets/71286321/d86ccb4c-d764-4278-8647-3374651790be">
<br>
<br>

**trades_per_insider**: Grouping all trades together per insider

<img width="175" alt="image" src="https://github.com/nruffini32/openinsider/assets/71286321/e8544d62-5064-4780-bfd8-a201b3396163">
