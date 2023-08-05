"""Script for starting the Frame Splitter server."""

import json
import logging
import boto3

from pathlib import Path

import requests
from flask import Flask, request, jsonify

app = Flask(__name__)
app.logger.setLevel(logging.DEBUG)  # Set the logging level to debug


def confirm_subscription(request_header, request_data):
    """Confirms the SNS subscription."""
    if request_header.get('x-amz-sns-message-type') == 'SubscriptionConfirmation':
        app.logger.info("Got request for confirming subscription")
        app.logger.info(request_header)
        # Extract the request data from the POST body

        subscribe_url = request_data['SubscribeURL']

        # Make an HTTP GET request to the SubscribeURL to confirm the subscription
        # This confirms the subscription with Amazon SNS
        # You can use any HTTP library of your choice (e.g., requests)

        app.logger.info(f"Going to URL: {subscribe_url} to confirm the subscription.")
        response = requests.get(subscribe_url)

        if response.status_code == 200:
            app.logger.info(f"Subscription confirmed. Code: {response.status_code}.")
            return jsonify({'message': 'SubscriptionConfirmed'})
        else:
            app.logger.warning(f"Failed to confirmed subscription. Code {response.status_code}.")
            return jsonify({'message': 'Failed to confirm subscription'}), 500


@app.route('/split-full-match-video', methods=['POST'])
def split_full_match_video():
    request_data = request.data.decode('utf-8')

    # Parse the JSON data into a Python dictionary
    try:
        data = json.loads(request_data)
    except json.JSONDecodeError as e:
        return jsonify({'error': str(e)}), 400

    confirm_subscription(request.headers, data)

    app.logger.info(f"Received Event: {data}.")

    # extract bucket and key
    message = json.loads(data["Message"])
    detail = message["detail"]
    bucket = detail["bucket"]["name"]
    object_key = detail["object"]["key"]

    video_dir = "temp-video"
    Path(video_dir).mkdir(parents=True, exist_ok=True)

    # download object
    s3 = boto3.client('s3')

    app.logger.info(f"Received following message: {message}")
    app.logger.info(f"Downloading Object: {object_key} from Bucket: {bucket}.")

    with open(f"video_dir/{object_key}", 'wb') as file:
        s3.download_fileobj(bucket, object_key, file)

    return jsonify({'message': 'Hello from the endpoint'}), 200


@app.route('/hello-world', methods=['GET'])
def hello_world():
    return "Hello World"


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
