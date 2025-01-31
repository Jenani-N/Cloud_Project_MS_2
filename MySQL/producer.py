from google.cloud import pubsub_v1      # pip install google-cloud-pubsub  ##to install
import glob                             # for searching for json file 
import json
import os
import numpy as np                      # pip install numpy    ##to install
import csv
import time

# Search the current directory for the JSON file (including the service account key) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];

# Set the project_id with your project ID
project_id="savvy-pad-448520-i8";
topic_name = "carLocationData";   # change it for your topic name if needed

# create a publisher and get the topic path for the publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Published messages with ordering keys to {topic_path}.")

# Getting the values from Labels.csv
with open("Labels.csv", mode='r') as file: #read file permission
    reader = csv.DictReader(file)

    try:
        for msg in reader:
            data_row = json.dumps(msg).encode('utf-8');    # serialize the message
            future = publisher.publish(topic_path, data_row); # push message to the topic

            # ensure that the publishing has been completed successfully
            future.result()
            print("The messages {} has been published successfully".format(msg))
            time.sleep(1)  # wait for 1s

    except:
        print("Keyboard Intrerrupt - Failed to publish the message")


    

        
      