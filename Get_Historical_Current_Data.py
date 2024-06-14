import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text
import mysql.connector

# Function to fetch stock data
def get_stock_data(symbol):
    try:
        stock = yf.Ticker(symbol)

        # Get historical data
        historical_data = stock.history(period="1mo")

        # Get current data
        current_data = stock.info

        if not historical_data.empty:
            # Add 'symbol' column to historical data
            historical_data['symbol'] = symbol

            # Reset index and rename 'Date' column to match MySQL table schema
            historical_data.reset_index(inplace=True)
            historical_data.rename(columns={'Date': 'date_time'}, inplace=True)

            # Reorder columns to match MySQL table schema and exclude 'Dividends' and 'Stock Splits'
            historical_data = historical_data[['date_time', 'symbol', 'Open', 'High', 'Low', 'Close', 'Volume']]

            # Convert current data to a DataFrame
            current_data_df = pd.DataFrame({
                'symbol': [symbol],
                'marketCap': [current_data.get('marketCap', None)],
                'forwardPE': [current_data.get('forwardPE', None)],
                'trailingPE': [current_data.get('trailingPE', None)],
                'dayHigh': [current_data.get('dayHigh', None)],
                'dayLow': [current_data.get('dayLow', None)],
                'fiftyTwoWeekHigh': [current_data.get('fiftyTwoWeekHigh', None)],
                'fiftyTwoWeekLow': [current_data.get('fiftyTwoWeekLow', None)],
                'dividendYield': [current_data.get('dividendYield', None)],
                'beta': [current_data.get('beta', None)],
                'primaryExchange': [current_data.get('primaryExchange', None)],
                'currency': [current_data.get('currency', None)]
            })

            return historical_data, current_data_df
        else:
            print(f"No historical data available for {symbol}")
            return None, None
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None, None

# Function to fetch company name based on symbol
def get_company_name(symbol, db_connection):
    cursor = db_connection.cursor()
    cursor.execute("SELECT name FROM companies WHERE ticker = %s", (symbol,))
    result = cursor.fetchone()
    cursor.close()
    if result:
        return result[0]  # Return the company name
    else:
        return None  # Handle case where symbol does not exist in database

# MySQL database connection details
db_user = 'root'
db_password = 'sanat123'
db_host = 'localhost'  # or your MySQL server IP/hostname
db_name = 'test_schema'

# Creating database connection
engine = create_engine(f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}')

# Establish MySQL connection using mysql.connector for company name retrieval
db_connection = mysql.connector.connect(
    host=db_host,
    user=db_user,
    password=db_password,
    database=db_name
)

nifty_50_symbols = [
    "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "HDFC.NS",
    "ICICIBANK.NS", "KOTAKBANK.NS", "HINDUNILVR.NS", "SBIN.NS", "BAJFINANCE.NS",
    "BHARTIARTL.NS", "ASIANPAINT.NS", "ITC.NS", "AXISBANK.NS", "LT.NS",
    "DMART.NS", "SUNPHARMA.NS", "ULTRACEMCO.NS", "TITAN.NS", "NESTLEIND.NS",
    "WIPRO.NS", "MARUTI.NS", "M&M.NS", "HCLTECH.NS", "NTPC.NS",
    "TECHM.NS", "POWERGRID.NS", "TATAMOTORS.NS", "INDUSINDBK.NS", "SBILIFE.NS",
    "TATASTEEL.NS", "GRASIM.NS", "BAJAJFINSV.NS", "ADANIGREEN.NS", "CIPLA.NS",
    "ONGC.NS", "HDFCLIFE.NS", "BPCL.NS", "JSWSTEEL.NS", "COALINDIA.NS",
    "BRITANNIA.NS", "HEROMOTOCO.NS", "SHREECEM.NS", "DABUR.NS", "ADANIPORTS.NS",
    "EICHERMOT.NS", "DIVISLAB.NS", "HINDALCO.NS", "UPL.NS", "APOLLOHOSP.NS"
]

for symbol in nifty_50_symbols:
    historical_data, current_data = get_stock_data(symbol)
    if historical_data is not None and current_data is not None:
        try:
            company_name = get_company_name(symbol, db_connection)
            if company_name:
                # Add company name column to historical_data
                historical_data['company_name'] = company_name

                with engine.begin() as conn:
                    # Check if historical data exists
                    result = conn.execute(text(f"SELECT COUNT(*) FROM HistoricalStockData WHERE symbol = :symbol"), {'symbol': symbol})
                    count = result.scalar()
                    if count > 0:
                        print("Delete old data and Insert new Data")
                        # Delete existing historical data for the symbol
                        conn.execute(text(f"DELETE FROM HistoricalStockData WHERE symbol = :symbol"), {'symbol': symbol})

                    # Insert new historical data
                    historical_data.to_sql(name='HistoricalStockData', con=conn, if_exists='append', index=False)
                    print(f"Historical stock data saved in MySQL for {symbol}")

                    # Add company name column to current_data
                    current_data['company_name'] = company_name

                    # Check if current data exists
                    result = conn.execute(text(f"SELECT COUNT(*) FROM CurrentStockData WHERE symbol = :symbol"), {'symbol': symbol})
                    count = result.scalar()
                    if count > 0:
                        # Delete existing current data for the symbol
                        conn.execute(text(f"DELETE FROM CurrentStockData WHERE symbol = :symbol"), {'symbol': symbol})

                    # Insert new current data
                    current_data.to_sql(name='CurrentStockData', con=conn, if_exists='append', index=False)
                    print(f"Current stock data saved in MySQL for {symbol}")
            else:
                print(f"No company name found for symbol {symbol}")

        except Exception as e:
            print(f"Error saving data to MySQL for {symbol}: {e}")
    else:
        print(f"Failed to retrieve stock data for {symbol}")

# Close the database connections
db_connection.close()
