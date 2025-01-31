from google.cloud import pubsub_v1      # pip install google-cloud-pubsub  ##to install
import glob                             # for searching for json file 
import base64
import json
import os
import numpy as np                      # pip install numpy    ##to install


# Search the current directory for the JSON file (including the service account key) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];

# Set the project_id with your project ID
project_id="savvy-pad-448520-i8";
topic_name = "imageRedis";   # change it for your topic name if needed

# create a publisher and get the topic path for the publisher
publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
publisher = pubsub_v1.PublisherClient( publisher_options=publisher_options)
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Published messages with ordering keys to {topic_path}.")

#Image folder source
image_src = "./Dataset_Occluded_Pedestrian"

# Geting images
images = glob.glob(os.path.join(image_src, "*.*"))

# Publish each image
for image_path in images:
    with open(image_path, "rb") as f:
        value = base64.b64encode(f.read())  # read the image and serizalize it to base64

    imageName = os.path.basename(image_path)  # key is filename
    future = publisher.publish(topic_path, value, ordering_key=imageName)
    future.result()

    print(f"The image {imageName} has been published successfully")
    

        
      