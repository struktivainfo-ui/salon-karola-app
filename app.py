from flask import Flask,send_from_directory,render_template
app=Flask(__name__)
@app.route('/')
def index():return render_template('base.html')
@app.route('/static/<path:p>')
def static_files(p):return send_from_directory('static',p)
