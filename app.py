from flask import Flask, jsonify, request
from flask_cors import CORS, cross_origin
from datetime import time
from datetime import datetime
from datetime import date
from pandas.io.json import json_normalize
import pandas
import pyodbc
import requests
import json
import numpy as np
import statistics
from sklearn.metrics import r2_score
from sklearn.metrics import mean_squared_error

app = Flask(__name__)

CORS(app)

@app.route('/stateboundaries',methods=['GET'])
def stateboundaries():
    from xml.etree import ElementTree as ET
import requests

    url = 'http://econym.org.uk/gmap/states.xml'
    page = requests.get(url)
    tree = ET.fromstring(page.content)
    items = tree.getchildren()
    boundaries = []
    for item in items:
        counter = 1
        rows = (item.getchildren())
        for row in rows:
            boundaries.append({
                'state':item.get('name'),
                'latitude':row.get('lat'),
                'longitude':row.get('lng'),
                'point_id':counter
            })
            counter += 1
    return jsonify({'data':boundaries})

if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0', port=80)