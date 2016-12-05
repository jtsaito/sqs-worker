// Example usage of worker package's constructor and StartPolling
package main

import (
	"log"
	"time"

	"github.com/jtsaito/sqs-worker/worker"
)

func main() {
	url := "https://sqs.eu-central-1.amazonaws.com/502414664542/test-queue-one"

	worker := worker.New(url, "eu-central-1", handleTask, 5*time.Second)
	worker.StartPolling()
}

func handleTask(payload string) {
	log.Println(payload)
}
