import yfinance as yf
import mysql.connector
from mysql.connector import Error
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Connect to MySQL database
try:
    db = mysql.connector.connect(
        host='localhost',
        user='root',
        password='sanat123',
        database='test_schema'
    )
except Error as e:
    logger.error(f"Error connecting to MySQL: {e}")
    exit()

# Function to fetch cash flow data
def get_cash_flow(symbol):
    try:
        logger.info(f"Fetching cash flow data for {symbol}...")
        stock = yf.Ticker(symbol)
        cash_flow = stock.cash_flow
        if not cash_flow.empty:
            logger.info(f"Cash flow data found for {symbol}")
            return cash_flow.to_dict()
        else:
            logger.info(f"No cash flow data available for {symbol}")
            return None
    except Exception as e:
        logger.error(f"Error fetching cash flow data for {symbol}: {e}")
        return None

# Function to fetch company name based on symbol
def get_company_name(symbol):
    try:
        cursor = db.cursor()
        cursor.execute("SELECT name FROM companies WHERE ticker = %s", (symbol,))
        result = cursor.fetchone()
        cursor.close()
        if result:
            return result[0]  # Return the company name
        else:
            return None  # Handle case where symbol does not exist in database
    except Error as e:
        logger.error(f"Error fetching company name for {symbol}: {e}")
        return None

# Function to insert or update cash flow data into the database
def insert_or_update_cash_flow_data(symbol, company_name, data):
    try:
        cursor = db.cursor()
        for fiscal_year, metrics in data.items():
            fiscal_year = fiscal_year.date()  # Convert to date if it's a datetime object
            fiscal_year_str = fiscal_year.strftime('%Y-%m-%d')  # Format as YYYY-MM-DD
            sql = """
                INSERT INTO cash_flow 
                (symbol, company_name, fiscal_year, free_cash_flow, repayment_of_debt, issuance_of_debt,
                issuance_of_capital_stock, capital_expenditure, end_cash_position, other_cash_adjustment,
                beginning_cash_position, effect_of_exchange_rate_changes, changes_in_cash, financing_cash_flow,
                cash_dividends_paid, common_stock_dividend_paid, net_common_stock_issuance, common_stock_issuance,
                net_issuance_payments_of_debt, net_short_term_debt_issuance, net_long_term_debt_issuance,
                long_term_debt_payments, long_term_debt_issuance, investing_cash_flow, net_investment_purchase_and_sale,
                net_ppe_purchase_and_sale, sale_of_ppe, purchase_of_ppe, operating_cash_flow, taxes_refund_paid,
                change_in_working_capital, change_in_other_working_capital, change_in_other_current_liabilities,
                change_in_other_current_assets, other_non_cash_items, stock_based_compensation, amortization_of_securities,
                depreciation_amortization_depletion, operating_gains_losses, gain_loss_on_investment_securities,
                gain_loss_on_sale_of_ppe, net_income_from_continuing_operations)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                company_name = VALUES(company_name),
                free_cash_flow = VALUES(free_cash_flow),
                repayment_of_debt = VALUES(repayment_of_debt),
                issuance_of_debt = VALUES(issuance_of_debt),
                issuance_of_capital_stock = VALUES(issuance_of_capital_stock),
                capital_expenditure = VALUES(capital_expenditure),
                end_cash_position = VALUES(end_cash_position),
                other_cash_adjustment = VALUES(other_cash_adjustment),
                beginning_cash_position = VALUES(beginning_cash_position),
                effect_of_exchange_rate_changes = VALUES(effect_of_exchange_rate_changes),
                changes_in_cash = VALUES(changes_in_cash),
                financing_cash_flow = VALUES(financing_cash_flow),
                cash_dividends_paid = VALUES(cash_dividends_paid),
                common_stock_dividend_paid = VALUES(common_stock_dividend_paid),
                net_common_stock_issuance = VALUES(net_common_stock_issuance),
                common_stock_issuance = VALUES(common_stock_issuance),
                net_issuance_payments_of_debt = VALUES(net_issuance_payments_of_debt),
                net_short_term_debt_issuance = VALUES(net_short_term_debt_issuance),
                net_long_term_debt_issuance = VALUES(net_long_term_debt_issuance),
                long_term_debt_payments = VALUES(long_term_debt_payments),
                long_term_debt_issuance = VALUES(long_term_debt_issuance),
                investing_cash_flow = VALUES(investing_cash_flow),
                net_investment_purchase_and_sale = VALUES(net_investment_purchase_and_sale),
                net_ppe_purchase_and_sale = VALUES(net_ppe_purchase_and_sale),
                sale_of_ppe = VALUES(sale_of_ppe),
                purchase_of_ppe = VALUES(purchase_of_ppe),
                operating_cash_flow = VALUES(operating_cash_flow),
                taxes_refund_paid = VALUES(taxes_refund_paid),
                change_in_working_capital = VALUES(change_in_working_capital),
                change_in_other_working_capital = VALUES(change_in_other_working_capital),
                change_in_other_current_liabilities = VALUES(change_in_other_current_liabilities),
                change_in_other_current_assets = VALUES(change_in_other_current_assets),
                other_non_cash_items = VALUES(other_non_cash_items),
                stock_based_compensation = VALUES(stock_based_compensation),
                amortization_of_securities = VALUES(amortization_of_securities),
                depreciation_amortization_depletion = VALUES(depreciation_amortization_depletion),
                operating_gains_losses = VALUES(operating_gains_losses),
                gain_loss_on_investment_securities = VALUES(gain_loss_on_investment_securities),
                gain_loss_on_sale_of_ppe = VALUES(gain_loss_on_sale_of_ppe),
                net_income_from_continuing_operations = VALUES(net_income_from_continuing_operations)
            """
            sql_values = (
                symbol, company_name, fiscal_year_str,
                float(metrics.get('Free Cash Flow', 0)),
                float(metrics.get('Repayment Of Debt', 0)),
                float(metrics.get('Issuance Of Debt', 0)),
                float(metrics.get('Issuance Of Capital Stock', 0)),
                float(metrics.get('Capital Expenditure', 0)),
                float(metrics.get('End Cash Position', 0)),
                float(metrics.get('Other Cash Adjustment', 0)),
                float(metrics.get('Beginning Cash Position', 0)),
                float(metrics.get('Effect Of Exchange Rate Changes', 0)),
                float(metrics.get('Changes In Cash', 0)),
                float(metrics.get('Financing Cash Flow', 0)),
                float(metrics.get('Cash Dividends Paid', 0)),
                float(metrics.get('Common Stock Dividend Paid', 0)),
                float(metrics.get('Net Common Stock Issuance', 0)),
                float(metrics.get('Common Stock Issuance', 0)),
                float(metrics.get('Net Issuance Payments Of Debt', 0)),
                float(metrics.get('Net Short Term Debt Issuance', 0)),
                float(metrics.get('Net Long Term Debt Issuance', 0)),
                float(metrics.get('Long Term Debt Payments', 0)),
                float(metrics.get('Long Term Debt Issuance', 0)),
                float(metrics.get('Investing Cash Flow', 0)),
                float(metrics.get('Net Investment Purchase And Sale', 0)),
                float(metrics.get('Net PPE Purchase And Sale', 0)),
                float(metrics.get('Sale Of PPE', 0)),
                float(metrics.get('Purchase Of PPE', 0)),
                float(metrics.get('Operating Cash Flow', 0)),
                float(metrics.get('Taxes Refund Paid', 0)),
                float(metrics.get('Change In Working Capital', 0)),
                float(metrics.get('Change In Other Working Capital', 0)),
                float(metrics.get('Change In Other Current Liabilities', 0)),
                float(metrics.get('Change In Other Current Assets', 0)),
                float(metrics.get('Other Non Cash Items', 0)),
                float(metrics.get('Stock Based Compensation', 0)),
                float(metrics.get('Amortization Of Securities', 0)),
                float(metrics.get('Depreciation Amortization Depletion', 0)),
                float(metrics.get('Operating Gains Losses', 0)),
                float(metrics.get('Gain Loss On Investment Securities', 0)),
                float(metrics.get('Gain Loss On Sale Of PPE', 0)),
                float(metrics.get('Net Income From Continuing Operations', 0))
            )
            cursor.execute(sql, sql_values)

        db.commit()
        logger.info(f"Successfully inserted/updated cash flow data for {symbol}")
    except Error as e:
        db.rollback()
        logger.error(f"Error inserting/updating cash flow data for {symbol}: {e}")
    finally:
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

# Fetch and insert or update data for each symbol
for symbol in nifty_50_symbols:
    company_name = get_company_name(symbol)
    if company_name:
        cash_flow_data = get_cash_flow(symbol)
        if cash_flow_data is not None:
            insert_or_update_cash_flow_data(symbol, company_name, cash_flow_data)
        else:
            logger.warning(f"No data to save for {symbol}")
    else:
        logger.warning(f"No company name found for symbol {symbol}")

# Close the database connection
db.close()
