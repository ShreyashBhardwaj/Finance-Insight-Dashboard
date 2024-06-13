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
        quarterly_income_stmt = stock.quarterly_financials
        yearly_income_stmt = stock.financials
        return quarterly_income_stmt.to_dict(), yearly_income_stmt.to_dict()
    except Exception as e:
        print(f"Error fetching income statement data for {symbol}: {e}")
        return None, None

# Function to fetch company name based on symbol
def get_company_name(symbol):
    cursor = db.cursor()
    cursor.execute("SELECT name FROM companies WHERE ticker = %s", (symbol,))
    result = cursor.fetchone()
    cursor.close()
    if result:
        return result[0]  # Return the company name
    else:
        return None  # Handle case where symbol does not exist in database

# Function to insert or update income statement data in the database
def insert_income_statement_data(symbol, data, statement_type):
    company_name = get_company_name(symbol)
    if company_name:
        cursor = db.cursor()
        for fiscal_period, metrics in data.items():
            fiscal_period = fiscal_period.to_pydatetime().date()  # Convert Timestamp to date

            # Check if data for this fiscal period already exists
            cursor.execute(f"""
                SELECT id FROM {statement_type}_income_statement 
                WHERE company_name = %s AND fiscal_period = %s
            """, (company_name, fiscal_period))
            existing_record = cursor.fetchone()

            if existing_record:
                # Update existing record
                sql = f"""
                    UPDATE {statement_type}_income_statement SET 
                    symbol = %s,
                    total_revenue = %s,
                    gross_profit = %s,
                    operating_income = %s,
                    net_income = %s,
                    ebit = %s,
                    diluted_eps = %s,
                    interest_expense = %s,
                    research_development = %s,
                    selling_general_administrative = %s
                    WHERE id = %s
                """
                values = (
                    symbol,
                    float(metrics.get('Total Revenue', 0)),
                    float(metrics.get('Gross Profit', 0)),
                    float(metrics.get('Operating Income', 0)),
                    float(metrics.get('Net Income', 0)),
                    float(metrics.get('EBIT', 0)),
                    float(metrics.get('Diluted EPS', 0)),
                    float(metrics.get('Interest Expense', 0)),
                    float(metrics.get('Research Development', 0)),
                    float(metrics.get('Selling General Administrative', 0)),
                    existing_record[0]  # id of the existing record
                )
            else:
                # Insert new record
                sql = f"""
                    INSERT INTO {statement_type}_income_statement 
                    (symbol, company_name, fiscal_period, total_revenue, gross_profit, operating_income, net_income, ebit, 
                    diluted_eps, interest_expense, research_development, selling_general_administrative)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                values = (
                    symbol, company_name, fiscal_period,
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
    else:
        print(f"No company name found for symbol {symbol}")


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
    quarterly_income_statement_data, yearly_income_statement_data = get_income_statement(symbol)
    if quarterly_income_statement_data is not None:
        insert_income_statement_data(symbol, quarterly_income_statement_data, 'quarterly')
    else:
        print(f"No quarterly data to save for {symbol}")
    if yearly_income_statement_data is not None:
        insert_income_statement_data(symbol, yearly_income_statement_data, 'yearly')
    else:
        print(f"No yearly data to save for {symbol}")

# Close the database connection
db.close()
