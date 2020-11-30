from flask import Flask, render_template, request, url_for, flash, redirect
import boto3
from io import StringIO
from pytrends.request import TrendReq
import pandas as pd
import time
import json
from datetime import datetime
import matplotlib.pyplot as plt


app = Flask(__name__, template_folder="templates")

def connect_to_s3():
    resource_type = "s3"
    access_key_id = ''
    secret_access_key = ''
    client = boto3.resource(resource_type, aws_access_key_id = access_key_id, aws_secret_access_key = secret_access_key)
    return client

def csv_on_s3(dataframe, filename, destination):
    """ Write the trends dataframe to a CSV on S3 """
    client = connect_to_s3()
    print("Writing {} records to {}".format(len(dataframe), filename))
    # Create buffer
    csv_buffer = StringIO()
    # Write dataframe to buffer
    dataframe.to_csv(csv_buffer, sep="|", index=False)
    # Create S3 object
    # s3_resource = boto3.resource("s3")
    # Write buffer to S3 object
    client.Object(destination, filename).put(Body=csv_buffer.getvalue())


def create_trends_dataframe(keywords, start_date, end_date, location):
    """
    Create a dataframe which would contain the trends data for the entered time period, keywords and location.
    """
    bucket_name = 'testuploadbucket1'
    pytrend = TrendReq(hl='en-GB', tz=360)
    geo_name = location

    keyword_list = [x.strip() for x in keywords.split(",")]

    timerange = start_date + ' ' + end_date
    dataset = []
    for x in range(0,len(keyword_list)):
        keywords = [keyword_list[x]]
        pytrend.build_payload(
        kw_list=keywords,
        cat=0,
        timeframe=timerange,
        geo=geo_name,
        gprop='')
        data = pytrend.interest_over_time()
        if not data.empty:
            data = data.drop(labels=['isPartial'],axis='columns')
            dataset.append(data)

    result = pd.concat(dataset, axis=1)
    result.reset_index(level=0, inplace=True)
    now = datetime.now()
    print("now =", now)
    # dd/mm/YY H:M:S
    dt_string = now.strftime("%Y%m%d-%H%M%S")
    file_name = "trends_" +dt_string+".csv"
    if result.empty:
        return "fail"
    else:
        csv_on_s3(result, file_name, bucket_name)
        return "success"

@app.route('/index', methods=['GET', 'POST'])
def index():

    with open('locations.json') as f:
      data = json.load(f)

    location_dict = {}

    for county in data['children']:
        location_dict[county['name']] = county['id']

    place_list = location_dict.keys()
    error = None
    location = 'US'
    message = None
    if request.method == "POST":
        keyword_list = request.form.get('keyword_list')
        start_date = request.form.get('start_date')
        end_date = request.form.get('end_date')
        location = request.form.get('option')
        status = create_trends_dataframe(keyword_list, start_date, end_date, location)

        return render_template('index.html', option_list = place_list, message = status )
    else:
        return render_template('index.html', option_list = place_list)

if __name__ == "__main__":
    app.run(debug= True)
