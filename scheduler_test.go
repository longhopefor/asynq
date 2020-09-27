// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/base"
)

func TestScheduler(t *testing.T) {
	tests := []struct {
		cronspec string
		task     *Task
		opts     []Option
		wait     time.Duration
		queue    string
		want     []*base.TaskMessage
	}{
		{
			cronspec: "@every 3s",
			task:     NewTask("task1", nil),
			opts:     []Option{MaxRetry(10)},
			wait:     10 * time.Second,
			queue:    "default",
			want: []*base.TaskMessage{
				{
					Type:    "task1",
					Payload: nil,
					Retry:   10,
					Timeout: int64(defaultTimeout.Seconds()),
					Queue:   "default",
				},
				{
					Type:    "task1",
					Payload: nil,
					Retry:   10,
					Timeout: int64(defaultTimeout.Seconds()),
					Queue:   "default",
				},
				{
					Type:    "task1",
					Payload: nil,
					Retry:   10,
					Timeout: int64(defaultTimeout.Seconds()),
					Queue:   "default",
				},
			},
		},
	}

	r := setup(t)

	for _, tc := range tests {
		scheduler := NewScheduler(getRedisConnOpt(t), nil)
		scheduler.Register(tc.cronspec, tc.task, tc.opts...)

		if err := scheduler.Start(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(tc.wait)
		if err := scheduler.Stop(); err != nil {
			t.Fatal(err)
		}

		got := asynqtest.GetPendingMessages(t, r, tc.queue)
		if diff := cmp.Diff(tc.want, got, asynqtest.IgnoreIDOpt); diff != "" {
			t.Errorf("mismatch found in queue %q: (-want,+got)\n%s", tc.queue, diff)
		}
	}
}

func TestStringifyOptions(t *testing.T) {
	now := time.Now()
	oneHourFromNow := now.Add(1 * time.Hour)
	twoHoursFromNow := now.Add(2 * time.Hour)
	tests := []struct {
		opts []Option
		want string
	}{
		{
			opts: []Option{MaxRetry(10)},
			want: "MaxRetry(10)",
		},
		{
			opts: []Option{Queue("custom"), Timeout(1 * time.Minute)},
			want: `Queue("custom"), Timeout(1m0s)`,
		},
		{
			opts: []Option{ProcessAt(oneHourFromNow), Deadline(twoHoursFromNow)},
			want: fmt.Sprintf("ProcessAt(%v), Deadline(%v)", oneHourFromNow, twoHoursFromNow),
		},
		{
			opts: []Option{ProcessIn(30 * time.Minute), Unique(1 * time.Hour)},
			want: "ProcessIn(30m0s), Unique(1h0m0s)",
		},
	}

	for _, tc := range tests {
		got := stringifyOptions(tc.opts)
		if got != tc.want {
			t.Errorf("got %v, want %v", got, tc.want)
		}
	}
}
