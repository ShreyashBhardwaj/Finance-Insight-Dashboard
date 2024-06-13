import yfinance as yf
import mysql.connector

# Connect to MySQL database
db = mysql.connector.connect(
    host='localhost',
    user='root',
    password='sanat123',
    database='test_schema'
)

# Function to fetch balance sheet data
def get_balance_sheet(symbol):
    try:
        stock = yf.Ticker(symbol)
        balance_sheet = stock.balance_sheet
        if not balance_sheet.empty:
            return balance_sheet.to_dict()
        else:
            print(f"No balance sheet data available for {symbol}")
            return None
    except Exception as e:
        print(f"Error fetching balance sheet data for {symbol}: {e}")
        return None

# Function to insert balance sheet data into the database
def insert_balance_sheet_data(symbol, data):
    cursor = db.cursor()
    for fiscal_year, metrics in data.items():
        fiscal_year = fiscal_year.year  # Directly extract the year from the Timestamp
        sql = """
            INSERT INTO balance_sheet 
            (symbol, fiscal_year, total_assets, total_liabilities, total_equity, current_assets, 
            current_liabilities, long_term_debt, short_term_debt, retained_earnings, net_income, 
            total_revenue, operating_income, net_cash_provided_by_operating_activities, 
            net_cash_used_for_investing_activities, net_cash_used_for_financing_activities)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = (
            symbol, fiscal_year,
            float(metrics.get('Total Assets', 0)),
            float(metrics.get('Total Liabilities Net Minority Interest', 0)),
            float(metrics.get('Total Equity Gross Minority Interest', 0)),
            float(metrics.get('Total Current Assets', 0)),
            float(metrics.get('Total Current Liabilities Net', 0)),
            float(metrics.get('Long Term Debt', 0)),
            float(metrics.get('Short Term Debt / Current Portion of Long Term Debt', 0)),
            float(metrics.get('Retained Earnings (Accumulated Deficit)', 0)),
            float(metrics.get('Net Income', 0)),
            float(metrics.get('Total Revenue', 0)),
            float(metrics.get('Operating Income', 0)),
            float(metrics.get('Net Cash from Operating Activities', 0)),
            float(metrics.get('Net Cash from Investing Activities', 0)),
            float(metrics.get('Net Cash from Financing Activities', 0))
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
    balance_sheet_data = get_balance_sheet(symbol)
    if balance_sheet_data is not None:
        insert_balance_sheet_data(symbol, balance_sheet_data)
    else:
        print(f"No data to save for {symbol}")

# Close the database connection
db.close()
#