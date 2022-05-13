package sandbox

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

type (
	BatchProcessEvent struct {
		Message string // serializable
		message string // not serializable
	}
	BatchingParam struct {
		BatchWaitThreshold time.Duration
		BatchMaxSize       int
		Events             []*BatchProcessEvent
	}
)

const (
	BatchWorkflowID          = "batch.workflow"
	BatchSignalName          = "batch.processing.event"
	BatchProcessingTaskQueue = "BATCH_PROCESING_TASK_QUEUE"
)

var (
	abandonedCartTimeout = 10 * time.Second
)

//https://community.temporal.io/t/collecting-results-for-bulk-operations/1541/12

// with select and draining, but not before exit
func BatchProcessWorkflow(ctx workflow.Context, param BatchingParam) error {
	logger := workflow.GetLogger(ctx)
	logger.Info(fmt.Sprint("batching workflow start"))

	signalChan := workflow.GetSignalChannel(ctx, BatchSignalName)
	for {
		var doneWaiting bool
		channelSelector := workflow.NewSelector(ctx)
		channelSelector.AddReceive(signalChan, func(c workflow.ReceiveChannel, more bool) {
			if !doneWaiting && !(len(param.Events) > param.BatchMaxSize) {
				var event BatchProcessEvent
				c.Receive(ctx, &event)
				logger.Info(fmt.Sprint("Received signal for event ", event))
				param.Events = append(param.Events, &event)
			} else {
				logger.Info(fmt.Sprint("Ignoring signal"))
			}
		})

		timerFuture := workflow.NewTimer(ctx, time.Second*30)
		channelSelector.AddFuture(timerFuture, func(f workflow.Future) {
			doneWaiting = true
		})

		for !doneWaiting && !(len(param.Events) > param.BatchMaxSize) {
			channelSelector.Select(ctx)
		}

		if len(param.Events) > 0 {
			logger.Info(fmt.Sprint("processing batch of length ", len(param.Events), fmt.Sprintf("%v", param.Events)))

			ao := workflow.ActivityOptions{
				StartToCloseTimeout: time.Minute,
			}
			ctx1 := workflow.WithActivityOptions(ctx, ao)

			err := workflow.ExecuteActivity(ctx1, BatchProcessingActivity, param.Events).Get(ctx, nil)
			if err != nil {
				logger.Error("Error batch processing %v", err)
			}
			param.Events = nil
			break
		}
	}

	// drain here and passed on to the future instance of workflow.
	for {
		var event BatchProcessEvent
		more := signalChan.ReceiveAsync(&event)
		//if ok {
		if more {
			workflow.GetLogger(ctx).Info("event received.", fmt.Sprintf("%v", event))
			param.Events = append(param.Events, &event)
		} else {
			break
		}

	}

	logger.Info(fmt.Sprint("Workflow completed "))
	return workflow.NewContinueAsNewError(ctx, BatchProcessWorkflow, param)

}

// With AwaitWithTimeout, no draining, but also no select
func BatchProcessWorkflow2(ctx workflow.Context, param BatchingParam) error {
	logger := workflow.GetLogger(ctx)

	signalChan := workflow.GetSignalChannel(ctx, BatchSignalName)

	//	var a *Activities
	var events []BatchProcessEvent

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
	}

	ctx = workflow.WithActivityOptions(ctx, ao)
	var event BatchProcessEvent
	for {
		workflow.GetLogger(ctx).Info("event received.")
		more, ok := signalChan.ReceiveAsyncWithMoreFlag(&event)
		if ok {
			events = append(events, event)
		}
		if more == false {
			break
		}
	}

	workflow.AwaitWithTimeout(ctx, param.BatchWaitThreshold, func() bool {
		return len(events) > int(param.BatchMaxSize)
	})

	err := workflow.ExecuteActivity(ctx, BatchProcessingActivity, len(events)).Get(ctx, nil)
	if err != nil {
		logger.Error("Error batch processing %v", err)
	}

	return workflow.NewContinueAsNewError(ctx, BatchProcessWorkflow, param)
}

// With drain on defered return
func BatchProcessWorkflow3(ctx workflow.Context, param BatchingParam) error {
	logger := workflow.GetLogger(ctx)

	signalChan := workflow.GetSignalChannel(ctx, BatchSignalName)

	childCtx, _ := workflow.WithCancel(ctx)

	var events []BatchProcessEvent

	defer func() {

		if !errors.Is(ctx.Err(), nil) {
			return
		}

		ao := workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute,
		}

		ctx = workflow.WithActivityOptions(ctx, ao)

		var event BatchProcessEvent
		for {
			more, ok := signalChan.ReceiveAsyncWithMoreFlag(&event)
			if ok {
				events = append(events, event)
			}
			if more == false {
				break
			}
		}

		err := workflow.ExecuteActivity(ctx, BatchProcessingActivity, len(events)).Get(ctx, nil)
		if err != nil {
			logger.Error("Error batch processing %v", err)
		}

	}()

	timerFuture := workflow.NewTimer(childCtx, param.BatchWaitThreshold)
	selector := workflow.NewSelector(ctx)

	selector.AddReceive(signalChan, func(channel workflow.ReceiveChannel, more bool) {
		workflow.GetLogger(ctx).Info("event received.")

		for {
			var event BatchProcessEvent
			more, ok := signalChan.ReceiveAsyncWithMoreFlag(&event)
			if ok {
				events = append(events, event)
			}
			if more == false {
				break
			}
		}
	})

	var timeoutExceeded bool

	selector.AddFuture(timerFuture, func(f workflow.Future) {
		workflow.GetLogger(ctx).Info("timeout exceeded.")
		timeoutExceeded = true
		return
	})

	if (len(events) < int(param.BatchMaxSize)) || !timeoutExceeded {
		workflow.GetLogger(ctx).Info("break.")
		selector.Select(ctx)
	}

	return workflow.NewContinueAsNewError(ctx, BatchProcessWorkflow, param)
}

func BatchProcessingActivity(ctx context.Context, events []BatchProcessEvent) (string, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("activity processing started.", "count", len(events))
	for _, v := range events {
		logger.Info("BatchProcessingActivity process", "event", v)
	}
	timeNeededToProcess := time.Second * time.Duration(rand.Intn(10))
	time.Sleep(timeNeededToProcess)
	logger.Info("activity done.", "duration", timeNeededToProcess)
	return fmt.Sprintf("processing done %v", len(events)), nil
}

func SendEmailActivity(ctx context.Context) error {
	activity.GetLogger(ctx).Info("SendEmailActivity sending notification email as the process takes long time.")
	return nil
}
