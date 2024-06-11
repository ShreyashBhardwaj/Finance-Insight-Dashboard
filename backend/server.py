from flask import Flask, jsonify, request, render_template, send_from_directory
from flask_cors import CORS
import mysql.connector
import os

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

# Configure Flask to use templates folder
app.template_folder = template_folder


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


@app.route('/')
def index():
    return render_template('index.html')


if __name__ == '__main__':
    app.run(debug=True)
