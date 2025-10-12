Market_Data is a personalized stock trading webpage(terminal) for screening, watchlisting, developing strategies, backtesting, portfolio tracking, signals/alerts/news,etc.

data_engine helps to provide the necessary parameters and market data in a data.duckdb file as a latest release.

script.py helps to fetch the market data from APIs(olhcv), clean them, compute the parameters/indicators, and create duckdb file with each symbol table for the values.

requirements.py helps to list the install dependencies for the script.py.

update_data.yml helps to cron job script.py sun-thu @6pm NPT and release the data.duckdb file as latest release.


fetch symbol
filter symbol
fetch symbol dolhcv

computes parameters
creates duckdb table 
updates the table each day
