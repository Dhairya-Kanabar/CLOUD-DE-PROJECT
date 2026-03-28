
from flask import Flask, render_template, request, redirect, url_for
import boto3

app = Flask(__name__)

# DynamoDB connection
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

marks_table = dynamodb.Table('marks')
student_table = dynamodb.Table('student')

@app.route('/')
def home():
    return render_template('index.html')


@app.route('/save_data', methods=['POST'])
def save_data():
    regno = request.form['regno']
    name = request.form['name']
    standard = request.form['class']
    math = request.form['math']
    science = request.form['science']
    computer = request.form['computer']

    # Save to student table
    student_table.put_item(
        Item={
            'id': regno,
            'name': name
        }
    )

    # Save to marks table
    marks_table.put_item(
        Item={
            'student_id': regno,
            'class': standard,
            'math': math,
            'science': science,
            'computer': computer
        }
    )

    return redirect(url_for('home'))

app.run(host='0.0.0.0', port=80)

