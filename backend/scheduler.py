import schedule
import time
import subprocess

# Define functions to run each task script
def run_task1():
    subprocess.run(["python", "Get_Historical_Current_Data.py"])

def run_task2():
    subprocess.run(["python", "Get_Balance_Sheet.py"])

def run_task3():
    subprocess.run(["python", "Get_CashFlow.py"])

def run_task4():
    subprocess.run(["python", "Get_Income_Statement.py"])

def run_task5():
    subprocess.run(["python", "Get_AssetProfile.py"])

# Schedule the tasks
schedule.every(5).minutes.do(run_task1)
schedule.every(1).hour.do(run_task2)
schedule.every().day.at("10:30").do(run_task3)

# Keep the scheduler running
while True:
    schedule.run_pending()
    time.sleep(1)
