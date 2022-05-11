package sandbox

import (
	"context"
	"log"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

func SignalBatchingTrigger(signalLabel string) error {
	// Create the client object just once per process
	c, err := client.NewClient(client.Options{
		HostPort: client.DefaultHostPort,
		Identity: "trigger",
	})
	if err != nil {
		log.Fatalln("unable to create Temporal client", err)
	}
	defer c.Close()

	searchAttributes := map[string]interface{}{}
	options := client.StartWorkflowOptions{
		ID:                    BatchWorkflowID, // uuid.New().String(),
		TaskQueue:             BatchProcessingTaskQueue,
		SearchAttributes:      searchAttributes,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
		Memo: map[string]interface{}{
			"description": "batch with signal or timeout",
		},
	}
	bpe := BatchProcessEvent{
		Message: signalLabel,
	}
	we, err := c.SignalWithStartWorkflow(context.Background(), BatchWorkflowID, BatchSignalName, bpe, options,
		BatchProcessWorkflow, BatchingParam{
			BatchWaitThreshold: time.Second * 30,
			BatchMaxSize:       3, // 4 per batch
		})
	if err != nil {
		log.Fatalln("Error sending the Signal", err)
	}

	log.Printf("\nWorkflowID: %s RunID: %s\n", we.GetID(), we.GetRunID())
	return err
}
