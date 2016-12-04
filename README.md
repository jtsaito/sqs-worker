# sqs-worker
A simple worker for SQS written in Go. The worker periodically polls SQS for jobs making this solution easily scalable.
The worker is meant for use in an AWS EC2 autoscaling group.
Alternatively, I can be scaled by AWS Elastic Beanstalk in the "web service" tier, but not in the worker tier.
