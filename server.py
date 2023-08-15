"""Script for starting the Frame Splitter server."""

import json
import logging
import os
from io import BytesIO

import boto3
import cv2

from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import requests
from flask import Flask, request, jsonify, current_app

app = Flask(__name__)
app.logger.setLevel(logging.DEBUG)  # Set the logging level to debug

# the executor pool used by the splitting endpoint
executor = ThreadPoolExecutor(2)

# the futures store. If a game is currently being processed, it will be stored here in the meantime.
futures = {}


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

    return jsonify({"message": "Header does not contain 'x-amz-sns-message-type': 'SubscriptionConfirmation'. No "
                               "subscription to confirm."}), 500


def emit_num_frames_event(game_id: str, num_frames: int):
    """Emits an event to the default event bus that contains the number of frames in the video for a given game ID.

    :arg
        game_id (str): the ID of the game to be placed inside the event.
        num_frames (int): the number of frames to be placed inside the event.

    """
    eventbridge_client = boto3.client('events', region_name='eu-north-1')
    event_data = {
        "game-id": game_id,
        "num-frames": num_frames
    }
    app.logger.info(f"Emitting event with data: {event_data}.")
    # PutEvents request to send the custom event
    try:
        response = eventbridge_client.put_events(
            Entries=[
                {
                    'Source': "frame-splitter",
                    'DetailType': "FramesAddedToS3Event",
                    'Detail': json.dumps(event_data),
                    'EventBusName': 'default'  # Replace with your EventBridge EventBusName
                }
            ]
        )
        app.logger.info(f"Event successfully emitted. {response}")
    except Exception as e:
        app.logger.warning(f"Could not emit event.", exc_info=e)


def delete_local_frame(frame_path: str):
    """Deletes the local version of the frame.

    :arg
        frame_path (str): the path to the frame.
    """
    try:
        app.logger.info(f"Deleting local version of frame from {frame_path}.")
        os.remove(frame_path)
        app.logger.info(f"File {frame_path} successfully deleted.")
    except OSError as e:
        app.logger.warning(f"Could not delete local frame {frame_path}.", exc_info=e)


def upload_frame(s3_client, frame, bucket_name: str, object_key: str):
    """Uploads a frame to the specified bucket with the given object key.

    :arg
        s3_client: the boto3 s3 client object.
        frame: a CV2 frame.
        bucket_name (str): the bucket to which to upload the frame.
        object_key (str): the name of the frame in the bucket.

    :return
        (bool) true if upload was successful, false if not.
    """
    img_bytes = frame.tobytes()

    # Specify S3 bucket details
    # save the frame in a folder named after the game name
    game_id = object_key.split(".")[0]
    frame_object_key = f"{game_id}/frame_{frame_count:04d}.jpg"

    # Upload the frame to S3
    metadata = {"game-id": game_id}
    app.logger.info(f"Uploading {frame_object_key} to {bucket_name}.")

    try:
        s3_client.upload_fileobj(BytesIO(img_bytes), bucket_name, frame_object_key, ExtraArgs={"Metadata": metadata})
        return True
    except Exception as e:
        app.logger.warning(f"Could not upload frame {frame_object_key} to bucket {bucket_name}.", exc_info=e)

    return False


def get_frames(video_path: str):
    """Generator for the frames at the provided path.

    :arg
        video_path (str): the path to the video from which to get the frames.
    """
    # Open the video file
    cap = cv2.VideoCapture(video_path)

    # Check if the video file was opened successfully
    if not cap.isOpened():
        app.logger.error(f"Could not open video file: {video_path}")

    # Loop through the frames
    while True:
        ret, frame = cap.read()

        # Break the loop if no more frames are available
        if not ret:
            break

        yield frame

    # Release the video capture object
    cap.release()

def split_video(bucket, object_key):
    """Splits the video located at the bucket and object location into frames and uploads the frames to S3.

    All frames are saved to local storage as an intermediate step, and are deleted afterwards if they upload to S3 bucket
    is successful.

    :arg
        bucket (str): the name of the bucket where to find the video.
        object_key (str): the object key of the video in the bucket.

    :return
        (int): the number of frames uploaded.
    """
    video_dir = "temp-video"
    Path(video_dir).mkdir(parents=True, exist_ok=True)
    video_path = f"{video_dir}/{object_key}"

    # download object
    s3 = boto3.client('s3')

    app.logger.info(f"Downloading Object: {object_key} from Bucket: {bucket}.")

    with open(video_path, 'wb') as file:
        s3.download_fileobj(bucket, object_key, file)
        app.logger.info("Download successful.")

    frame_dir = "frames"
    Path(frame_dir).mkdir(parents=True, exist_ok=True)
    bucket_name = "nba-match-frames"

    frame_count = 0

    app.logger.info("Going through frames of the video.")

    for frame in get_frames(video_path):
        frame_count += 1
        frame_name = f"{object_key}_frame_{frame_count:04d}.jpg"
        local_frame_path = f'{frame_dir}/{frame_name}'
        cv2.imwrite(local_frame_path, frame)

        if upload_frame(s3, frame, bucket_name, frame_name):
            delete_local_frame(local_frame_path)

    app.logger.info(f"Uploaded {frame_count} frames to {bucket_name}.")
    return frame_count


def split_and_emit(bucket, object_key, game_id):
    frame_count = split_video(bucket, object_key)
    emit_num_frames_event(game_id, frame_count)


@app.route('/health', methods=["GET"])
def health_check():
    return jsonify({"message": "Health Check OK"}), 200


@app.route('/split-full-match-video', methods=['POST'])
def split_full_match_video():
    """Flask endpoint that splits a video into frames and uploads the frames to an S3 bucket.

    This endpoint is usually triggered by an AWS event, emitted when the video is added to an S3 bucket and then sent
    to this service by AWS SNS. This endpoint also confirms the SNS subscription.
    """
    request_data = request.data.decode('utf-8')

    # Parse the JSON data into a Python dictionary
    try:
        data = json.loads(request_data)
    except json.JSONDecodeError as e:
        return jsonify({'error': str(e)}), 400

    # if the subscription is confirmed, return after it
    if request.headers.get('x-amz-sns-message-type') == 'SubscriptionConfirmation':
        return confirm_subscription(request.headers, data)

    app.logger.info(f"Received Event: {data}.")

    # extract bucket and key
    message = json.loads(data["Message"])
    app.logger.info(f"Received following message: {message}")

    if message["detail-type"] == "Object Created":
        app.logger.info("Received object created message.")
        detail = message["detail"]
        bucket = detail["bucket"]["name"]
        object_key = detail["object"]["key"]

        # the name of the video file is the game ID
        game_id = object_key.split(".")[0]

        # if task is still running, ignore the request
        if game_id in futures:
            if not futures[game_id].done():
                app.logger.info(f"The file {game_id} is already being processed.")
                return jsonify({"message": "Game file is already being processed."}), 200
            else:
                app.logger.info(f"The file {game_id} finished processing.")
                del futures[game_id]

        app.logger.info(f"Starting splitting of video {game_id}.")

        future = executor.submit(split_and_emit, bucket, object_key, game_id)
        futures[game_id] = future

        return jsonify({'message': 'Game file in process'}), 200

    return jsonify({"message": "Invalid request"}), 400


@app.route('/hello-world', methods=['GET'])
def hello_world():
    return "Hello World"


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
