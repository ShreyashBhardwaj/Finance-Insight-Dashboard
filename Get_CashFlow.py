import yfinance as yf
import mysql.connector

# Connect to MySQL database
db = mysql.connector.connect(
    host='localhost',
    user='root',
    password='sanat123',
    database='test_schema'
)

# Function to fetch cash flow data
def get_cash_flow(symbol):
    try:
        stock = yf.Ticker(symbol)
        cash_flow = stock.cash_flow
        if not cash_flow.empty:
            return cash_flow.to_dict()
        else:
            print(f"No cash flow data available for {symbol}")
            return None
    except Exception as e:
        print(f"Error fetching cash flow data for {symbol}: {e}")
        return None

# Function to insert cash flow data into the database
def insert_cash_flow_data(symbol, data):
    cursor = db.cursor()
    for fiscal_year, metrics in data.items():
        fiscal_year = fiscal_year.year  # Directly extract the year from the Timestamp
        sql = """
            INSERT INTO cash_flow 
            (symbol, fiscal_year, net_cash_provided_by_operating_activities, net_cash_used_for_investing_activities, 
            net_cash_used_for_financing_activities, change_in_cash_and_cash_equivalents)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        values = (
            symbol, fiscal_year,
            float(metrics.get('Net Cash Provided By Operating Activities', 0)),
            float(metrics.get('Net Cash Used For Investing Activities', 0)),
            float(metrics.get('Net Cash Used For Financing Activities', 0)),
            float(metrics.get('Change In Cash And Cash Equivalents', 0))
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
    cash_flow_data = get_cash_flow(symbol)
    if cash_flow_data is not None:
        insert_cash_flow_data(symbol, cash_flow_data)
    else:
        print(f"No data to save for {symbol}")

# Close the database connection
db.close()
#