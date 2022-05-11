package sandbox

import (
	"log"
	"testing"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// hosting workflow and activity
func TestSignalBatchingWorkflow(t *testing.T) {
	// Create the client object just once per process
	c, err := client.NewClient(client.Options{
		HostPort: client.DefaultHostPort,
	})
	if err != nil {
		log.Fatalln("unable to create Temporal client", err)
	}
	defer c.Close()
	// This worker hosts both Workflow and Activity functions
	w := worker.New(c, BatchProcessingTaskQueue, worker.Options{})
	w.RegisterWorkflow(BatchProcessWorkflow)
	w.RegisterActivity(BatchProcessingActivity)

	// Start listening to the Task Queue
	err = w.Run(worker.InterruptCh())
	if err != nil {
		log.Fatalln("unable to start Worker", err)
	}
}
