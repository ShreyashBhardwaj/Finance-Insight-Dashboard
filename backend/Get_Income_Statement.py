import yfinance as yf
import mysql.connector
from datetime import datetime

# Connect to MySQL database
db = mysql.connector.connect(
    host='localhost',
    user='root',
    password='sanat123',
    database='test_schema'
)

# Function to fetch quarterly income statement data
def get_income_statement(symbol):
    try:
        stock = yf.Ticker(symbol)
        income_stmt = stock.quarterly_financials
        if not income_stmt.empty:
            return income_stmt.to_dict()
        else:
            print(f"No income statement data available for {symbol}")
            return None
    except Exception as e:
        print(f"Error fetching income statement data for {symbol}: {e}")
        return None

# Function to insert income statement data into the database
def insert_income_statement_data(symbol, data):
    cursor = db.cursor()
    for fiscal_period, metrics in data.items():
        fiscal_period = fiscal_period.to_pydatetime().date()  # Convert Timestamp to date
        sql = """
            INSERT INTO quarterly_income_statement 
            (symbol, fiscal_period, total_revenue, gross_profit, operating_income, net_income, ebit, 
            diluted_eps, interest_expense, research_development, selling_general_administrative)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = (
            symbol, fiscal_period,
            float(metrics.get('Total Revenue', 0)),
            float(metrics.get('Gross Profit', 0)),
            float(metrics.get('Operating Income', 0)),
            float(metrics.get('Net Income', 0)),
            float(metrics.get('EBIT', 0)),
            float(metrics.get('Diluted EPS', 0)),
            float(metrics.get('Interest Expense', 0)),
            float(metrics.get('Research Development', 0)),
            float(metrics.get('Selling General Administrative', 0))
        )
        cursor.execute(sql, values)
    db.commit()
    cursor.close()

# List of Nifty 50 symbols
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

# Process each symbol
for symbol in nifty_50_symbols:
    income_statement_data = get_income_statement(symbol)
    if income_statement_data is not None:
        insert_income_statement_data(symbol, income_statement_data)
    else:
        print(f"No data to save for {symbol}")

# Close the database connection
db.close()
