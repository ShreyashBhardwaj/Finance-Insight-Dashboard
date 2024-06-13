import yfinance as yf
import mysql.connector

# Connect to MySQL database
db = mysql.connector.connect(
    host='localhost',
    user='root',
    password='sanat123',
    database='test_schema'
)

# Function to fetch asset profile data
def get_asset_profile(symbol):
    try:
        stock = yf.Ticker(symbol)
        asset_profile = stock.info

        if asset_profile:
            return asset_profile
        else:
            print(f"No asset profile data available for {symbol}")
            return None
    except Exception as e:
        print(f"Error fetching asset profile data for {symbol}: {e}")
        return None

# Function to insert asset profile data into the database
def insert_asset_profile_data(symbol, data):
    cursor = db.cursor()
    sql = """
        INSERT INTO asset_profile 
        (symbol, address1, city, state, country, phone, website, industry, sector, 
        full_time_employees, long_business_summary)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = (
        symbol, data.get('address1'), data.get('city'), data.get('state'),
        data.get('country'), data.get('phone'), data.get('website'),
        data.get('industry'), data.get('sector'), data.get('fullTimeEmployees'),
        data.get('longBusinessSummary')
    )
    cursor.execute(sql, values)
    db.commit()
    cursor.close()

# List of symbols to fetch data for
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

# Fetch and insert data for each symbol
for symbol in nifty_50_symbols:
    asset_profile_data = get_asset_profile(symbol)
    if asset_profile_data is not None:
        insert_asset_profile_data(symbol, asset_profile_data)
    else:
        print(f"No data to save for {symbol}")

# Close the database connection
db.close()
