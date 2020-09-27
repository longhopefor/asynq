// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/log"
	"github.com/hibiken/asynq/internal/rdb"
	"github.com/robfig/cron/v3"
)

// A Scheduler kicks off tasks at regular intervals based on the user defined schedule.
type Scheduler struct {
	host   string
	pid    int
	id     uuid.UUID
	status *base.ServerStatus
	logger *log.Logger
	client *Client
	rdb    *rdb.RDB
	cron   *cron.Cron
	done   chan struct{}
}

// NewScheduler creates a new Scheduler instance given the redis connection option.
// opts is an optional parameter, defaults will be used if opts is set to nil
func NewScheduler(r RedisConnOpt, opts *SchedulerOpts) *Scheduler {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown-host"
	}
	if opts == nil {
		opts = &SchedulerOpts{}
	}
	logger := log.NewLogger(opts.Logger)
	loglevel := opts.LogLevel
	if loglevel == level_unspecified {
		loglevel = InfoLevel
	}
	logger.SetLevel(toInternalLogLevel(loglevel))
	loc := opts.Location
	if loc == nil {
		loc = time.UTC
	}
	return &Scheduler{
		host:   host,
		pid:    os.Getpid(),
		id:     uuid.New(),
		status: base.NewServerStatus(base.StatusIdle),
		logger: logger,
		client: NewClient(r),
		rdb:    rdb.NewRDB(createRedisClient(r)),
		cron:   cron.New(cron.WithLocation(loc)),
		done:   make(chan struct{}),
	}
}

// SchedulerOpts specifies scheduler options.
type SchedulerOpts struct {
	// Logger specifies the logger used by the scheduler instance.
	//
	// If unset, the default logger is used.
	Logger Logger

	// LogLevel specifies the minimum log level to enable.
	//
	// If unset, InfoLevel is used by default.
	LogLevel LogLevel

	// Location specifies the time zone location.
	//
	// If unset, the UTC time zone (time.UTC) is used.
	Location *time.Location

	// TODO: Add ErrorHandler
}

type enqueueJob struct {
	task   *Task
	opts   []Option
	client *Client
	logger *log.Logger

	// TODO: Remove this field if we can get this from cron.Entry
	cronspec string
}

func (j *enqueueJob) Run() {
	res, err := j.client.Enqueue(j.task, j.opts...)
	if err != nil {
		j.logger.Errorf("scheduler could not enqueue task %+v: %v", j.task, err)
		return
	}
	j.logger.Infof("scheduler enqueued task: %+v", res)
}

// Register registers a task to be enqueued with given schedule specified by the cronspec.
func (s *Scheduler) Register(cronspec string, task *Task, opts ...Option) {
	job := &enqueueJob{task, opts, s.client, s.logger, cronspec}
	// TODO: should we check for error returned here?
	s.cron.AddJob(cronspec, job)
}

// Run starts the scheduler until an os signal to exit the program is received.
// It returns an error if scheduler is already running or has been stopped.
func (s *Scheduler) Run() error {
	if err := s.Start(); err != nil {
		return err
	}
	s.waitForSignals()
	return s.Stop()
}

// Start starts the scheduler.
// It returns an error if the scheduler is already running or has been stopped.
func (s *Scheduler) Start() error {
	switch s.status.Get() {
	case base.StatusRunning:
		return fmt.Errorf("asynq: the scheduler is already running")
	case base.StatusStopped:
		return fmt.Errorf("asynq: the scheduler has been stopped")
	}
	s.cron.Start()
	go s.runHeartbeater()
	s.status.Set(base.StatusRunning)
	return nil
}

// Stop stops the scheduler.
// It returns an error if the scheduler is not currently running.
func (s *Scheduler) Stop() error {
	if s.status.Get() != base.StatusRunning {
		return fmt.Errorf("asynq: the scheduler is not running")
	}
	close(s.done) // signal heartbeater to stop
	ctx := s.cron.Stop()
	<-ctx.Done()
	s.client.Close()
	s.status.Set(base.StatusStopped)
	return nil
}

func (s *Scheduler) runHeartbeater() {
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-s.done:
			s.logger.Debugf("Scheduler heatbeater shutting down")
			s.rdb.ClearSchedulerEntries(s.host, s.pid, s.id.String())
			return
		case <-ticker.C:
			s.beat()
		}
	}
}

// beat writes a snapshot of entries to redis.
func (s *Scheduler) beat() {
	var entries []*base.SchedulerEntry
	for _, entry := range s.cron.Entries() {
		job, ok := entry.Job.(*enqueueJob)
		if !ok {
			fmt.Println("could not type assert to enqueueJob")
			continue
		}
		e := &base.SchedulerEntry{
			Spec:    job.cronspec,
			Type:    job.task.Type,
			Payload: job.task.Payload.data,
			Opts:    stringifyOptions(job.opts),
			Next:    entry.Next,
			Prev:    entry.Prev,
		}
		entries = append(entries, e)
	}
	s.logger.Debugf("Writing entries %v", entries)
	if err := s.rdb.WriteSchedulerEntries(s.host, s.pid, s.id.String(), entries, 5*time.Second); err != nil {
		s.logger.Warnf("scheduler could not write heartbeat data: %v", err)
	}
}

func stringifyOptions(opts []Option) string {
	var res []string
	for _, opt := range opts {
		switch opt := opt.(type) {
		case retryOption:
			res = append(res, fmt.Sprintf("MaxRetry(%d)", int(opt)))
		case queueOption:
			res = append(res, fmt.Sprintf("Queue(%q)", string(opt)))
		case timeoutOption:
			res = append(res, fmt.Sprintf("Timeout(%v)", time.Duration(opt)))
		case deadlineOption:
			res = append(res, fmt.Sprintf("Deadline(%v)", time.Time(opt)))
		case uniqueOption:
			res = append(res, fmt.Sprintf("Unique(%v)", time.Duration(opt)))
		case processAtOption:
			res = append(res, fmt.Sprintf("ProcessAt(%v)", time.Time(opt)))
		case processInOption:
			res = append(res, fmt.Sprintf("ProcessIn(%v)", time.Duration(opt)))
		default:
			// ignore unexpected option
		}
	}
	return strings.Join(res, ", ")
}
