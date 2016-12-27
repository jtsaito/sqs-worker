package worker

import (
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

/* Worker is a simple worker for SQS written in Go. The worker periodically polls SQS for jobs making this solution easily
scalable. The worker is meant for use in an AWS EC2 autoscaling group. Alternatively, I can be scaled by AWS
Elastic Beanstalk in the "web service" tier, but not in the worker tier. */
type Worker struct {
	url              string
	region           string
	handler          func(string)
	sqsClient        *sqs.SQS
	pollingIntervall time.Duration
	*log.Logger      // embed logger
}

/* New is a constructor */
func New(url, region string, handler func(string), pollingIntervall time.Duration) (worker *Worker) {
	logger := log.New(os.Stderr, "[SQS Worker] ", log.Lshortfile)

	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		logger.Println("failed to create session,", err)
		return
	}

	cli := sqs.New(sess)

	return &Worker{url, region, handler, cli, pollingIntervall, logger}
}

/* StartPolling starts polling SQS. When a message is found, it is passed to the handler. */
func (worker *Worker) StartPolling() {
	for {
		<-time.After(worker.pollingIntervall)
		worker.handleMessage()
	}
}

func (worker *Worker) handleMessage() {
	receiptHandle, payload := worker.readFromQueue()

	worker.Println("message received: ", payload)

	if receiptHandle != "" {
		worker.handler(payload)
		worker.deleteMessage(receiptHandle)
	}
}

func (worker *Worker) readFromQueue() (receiptHandle, payload string) {
	params := &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(worker.url),
		AttributeNames: []*string{
			aws.String("QueueAttributeName"),
		},
		MaxNumberOfMessages: aws.Int64(1),
		MessageAttributeNames: []*string{
			aws.String("All"),
		},
		VisibilityTimeout: aws.Int64(1),
		WaitTimeSeconds:   aws.Int64(1),
	}
	resp, err := worker.sqsClient.ReceiveMessage(params)

	if err != nil {
		worker.Println("reading")
		worker.Println(err.Error())
		return receiptHandle, ""
	}

	if len(resp.Messages) == 0 {
		return "", ""
	}

	payload = *resp.Messages[0].Body
	receiptHandle = *resp.Messages[0].ReceiptHandle

	return receiptHandle, payload
}

func (worker *Worker) deleteMessage(receiptHandle string) {
	params := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(worker.url),
		ReceiptHandle: aws.String(receiptHandle),
	}
	resp, err := worker.sqsClient.DeleteMessage(params)

	if err != nil {
		worker.Println("deleting")
		worker.Println(err.Error())
		return
	}

	worker.Println(resp)
}

// SendMessage sends a message.
func (worker *Worker) SendMessage(payload string) error {
	params := &sqs.SendMessageInput{
		QueueUrl:     aws.String(worker.url),
		DelaySeconds: aws.Int64(1),
		MessageBody:  aws.String(payload),
	}

	_, err := worker.sqsClient.SendMessage(params)

	return err
}
