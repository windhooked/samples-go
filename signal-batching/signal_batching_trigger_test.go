package sandbox

import (
	"testing"
)

//Expecting to see 2 workflows executions, with 1 batching activity executed
// the last workflow shold have 3 signal in the queue, and process on timeout

func TestSignalBatchingTrigger(t *testing.T) {
	type args struct {
		signalLabel string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{name: "1", args: args{signalLabel: "1"}, wantErr: false},
		{name: "2", args: args{signalLabel: "2"}, wantErr: false},
		{name: "3", args: args{signalLabel: "3"}, wantErr: false},
		{name: "4", args: args{signalLabel: "4"}, wantErr: false},
		{name: "5", args: args{signalLabel: "5"}, wantErr: false},
		{name: "6", args: args{signalLabel: "6"}, wantErr: false},
		{name: "7", args: args{signalLabel: "7"}, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := SignalBatchingTrigger(tt.args.signalLabel); (err != nil) != tt.wantErr {
				t.Errorf("SignalBatchingTrigger() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
