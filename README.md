# Purpose
Service that receives an NBA video and splits it into individual frames.

# How it works
The service is a simple `flask` HTTP web server on port 5000 that can be added as a subscriber to an SNS topic. The events it expects are
S3 events for the bucket where full NBA videos are being added.

The events are routed to the endpoint `/split-full-match-video` by a POST request. The function attached to this endpoint
then:

* downloads the video that is mentioned in the event.
* saves the individual frames of the video.
* uploads the frames to the `nba-match-frames` bucket.

# How to run it
First, make sure that

1. The S3 bucket that generates the events allows the user of the service to read objects.
2. The S3 bucket publishes events to EventBridge.
3. There is an EventBridge rule that listens to the source bucket and sends to the SNS topic.
4. The SNS topic has a subscription for this service.

This service needs to be run with an `.env` file that has the following

```
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=...
```