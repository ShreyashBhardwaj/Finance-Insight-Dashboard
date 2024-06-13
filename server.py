from flask import Flask, jsonify, request, render_template, redirect, url_for
from flask_cors import CORS
import mysql.connector
import os
import schedule
import time
import subprocess
import threading

app = Flask(__name__)
CORS(app)

# Connect to MySQL database
db = mysql.connector.connect(
    host='localhost',
    user='root',
    password='sanat123',
    database='test_schema'
)

# Define paths to static and template folders
static_folder = os.path.abspath('E:\\PROGRAMS AND PROJECTS\\Finance Insights Dashboard\\static')
template_folder = os.path.abspath('E:\\PROGRAMS AND PROJECTS\\Finance Insights Dashboard\\templates')

# Configure Flask to serve static files
app.static_folder = static_folder
app.template_folder = template_folder


# Flask route to search for companies
@app.route('/search')
def search():
    search_query = request.args.get('q')
    if not search_query:
        return jsonify({'error': 'Missing search query'}), 400

    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM companies WHERE name LIKE %s", ('%' + search_query + '%',))
    results = cursor.fetchall()
    cursor.close()

    return jsonify(results)


# Flask route for index
@app.route('/')
def index():
    return render_template('index.html')


# Flask route for dashboard
@app.route('/dashboard')
def dashboard():
    company_name = request.args.get('company')

    # Fetch data for the dashboard from the database (example)
    cursor = db.cursor(dictionary=True)

    # Example queries to fetch data from the database based on company_name
    cursor.execute("SELECT * FROM CurrentStockData WHERE company_name = %s", (company_name,))
    company_data = cursor.fetchone()  # Assuming we expect only one row

    cursor.execute("SELECT * FROM HistoricalStockData WHERE company_name = %s", (company_name,))
    historical_data = cursor.fetchall()

    cursor.execute("SELECT * FROM quarterly_income_statement WHERE company_name = %s", (company_name,))
    quarterly_results = cursor.fetchall()

    cursor.execute("SELECT * FROM yearly_income_statement WHERE company_name = %s", (company_name,))
    income_statement = cursor.fetchall()

    cursor.execute("SELECT * FROM balance_sheet WHERE company_name = %s", (company_name,))
    balance_sheet = cursor.fetchall()

    cursor.execute("SELECT * FROM cash_flow WHERE company_name = %s", (company_name,))
    cash_flow = cursor.fetchall()

    cursor.close()

    # Render dashboard.html with data
    return render_template('dashboard.html',
                           company_name=company_name,
                           company_data=company_data,
                           historical_data=historical_data,
                           quarterly_results=quarterly_results,
                           income_statement=income_statement,
                           balance_sheet=balance_sheet,
                           cash_flow=cash_flow)


# Scheduler tasks
def run_task(script_name):
    try:
        subprocess.run(["python", script_name], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_name}: {e}")


def run_task1():
    run_task("Get_Historical_Current_Data.py")


def run_task2():
    run_task("Get_Balance_Sheet.py")


def run_task3():
    run_task("Get_CashFlow.py")


def run_task4():
    run_task("Get_Income_Statement.py")


def run_task5():
    run_task("Get_AssetProfile.py")


# Function to schedule tasks
def schedule_tasks():
    schedule.every(35).minutes.do(run_task1)
    schedule.every(1).hour.do(run_task2)
    schedule.every(1).hour.do(run_task3)
    schedule.every(1).hour.do(run_task4)
    schedule.every(1).hour.do(run_task5)

    while True:
        schedule.run_pending()
        time.sleep(1)


# Function to start scheduler in a separate thread
def start_scheduler():
    scheduler_thread = threading.Thread(target=schedule_tasks)
    scheduler_thread.daemon = True
    scheduler_thread.start()


if __name__ == '__main__':
    # Start the scheduler
    start_scheduler()

    # Run the Flask app
    app.run(debug=True)
