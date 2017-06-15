// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cassandra

import (
	"errors"
	"os"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/uber/cherami-server/common/configure"
)

// BackupCron represents a background
// job that schedules backup tasks based
// on a pre-defined schedule
type BackupCron struct {
	cfg   *BackupToolConfig
	quitC <-chan struct{}
}

const defaultTimeZone = "UTC"

// azToTimeZone maps a zone name to time zone
var azToTimeZone = map[string]string{
	"dca1": "America/New_York",
	"sjc1": "America/Los_Angeles",
	"pek1": "Asia/Shanghai",
	"pvg1": "Asia/Shanghai",
}

var errTimedout = errors.New("Task timed out")
var errLoadSecret = errors.New("Error loading secret yaml")

const maxOverDueTime = time.Hour * 16 // if backup is overdue by 16 hrs, wait until next day
const taskTimeout = time.Hour * 6     // max time for backup task to complete
const dateFormat = "20060102"

// NewBackupCron returns a cron task that
// schedules backup based on a schedule
func NewBackupCron(svc string, cfg *BackupToolConfig) *BackupCron {
	return &BackupCron{
		cfg:   cfg,
		quitC: make(chan struct{}),
	}
}

// Run runs the cron task
func (cron *BackupCron) Run() {

	var hostname string
	var dc = configure.NewCommonConfigure().GetDatacenter()
	var deployment = cron.cfg.Deployment
	var rootDir = cron.cfg.RootDirPath

	log.Infof("BackupCron started, dc=%v, deployment=%v, root=%v, config=%+v", dc, deployment, rootDir, cron.cfg)

	hostname, err := os.Hostname()
	if err != nil {
		log.Errorf("os.Hostname() failed, err=%v", err)
		return
	}

	secret := cron.loadSecrets()
	if secret == nil {
		log.Error("Error loading secrets")
		return
	}

	tz, ok := azToTimeZone[dc]
	if !ok {
		tz = defaultTimeZone
		log.Infof("Unrecognized DC, defaulting to UTC")
	}

	var nextBackupTime = cron.getNextBackupTime(tz)

	for !isQuit(cron.quitC) {

		now := time.Now()

		if now.Before(nextBackupTime) {
			d := nextBackupTime.Sub(now)
			log.Infof("Sleeping until next backup at %v", nextBackupTime)
			sleep(cron.quitC, d)
			continue
		}

		d := now.Sub(nextBackupTime)
		if d > maxOverDueTime {
			nextBackupTime = nextBackupTime.Add(time.Hour * 24)
			log.Infof("Backup possibly overdue for day, will skip it")
			continue
		}

		date := nextBackupTime.Format(dateFormat)

		e := cron.runBackup(hostname, date, secret)
		if e == nil {
			nextBackupTime = nextBackupTime.Add(time.Hour * 24)
		}

		sleep(cron.quitC, time.Minute) // Spin atmost once a min
	}
}

// ScheduleNow schedules a one time back up immediately
func (cron *BackupCron) ScheduleNow(date string) error {
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	secret := cron.loadSecrets()
	if secret == nil {
		return errLoadSecret
	}
	return cron.runBackup(hostname, date, secret)
}

func (cron *BackupCron) runBackup(hostname string, date string, secret *Secret) error {

	log.Infof("Scheduling backup, date=%v", date)

	var err error

	task, err := NewBackupTask(cron.cfg, date, hostname, secret)
	if err != nil {
		log.Errorf("Error creating backup task, err=%v", err)
		return err
	}

	var doneC = make(chan struct{})

	func() {
		defer close(doneC)
		err = task.Run()
		if err != nil {
			log.Errorf("Backup task failed, err=%v", err)
			return
		}
		log.Infof("Backup task succeeded, date=%v", date)
	}()

	select {
	case <-doneC:
		return err
	case <-cron.quitC:
		return nil
	case <-time.After(taskTimeout):
		log.Errorf("Backup task timed out")
		task.Stop(time.Minute)
		return errTimedout
	}
}

func (cron *BackupCron) getNextBackupTime(tz string) time.Time {
	loc, err := time.LoadLocation(tz)
	if err != nil {
		loc = time.UTC
		log.Errorf("time.LoadLocation(%v) failed, err=%v, defaulting to UTC", tz, err)
	}
	localTime := time.Now().In(loc)
	backupTime := time.Date(localTime.Year(), localTime.Month(), localTime.Day(), cron.cfg.Backup.HourOfDay, 0, 0, 0, loc)
	return backupTime.In(time.UTC)
}

func (cron *BackupCron) loadSecrets() *Secret {
	for !isQuit(cron.quitC) {
		secret, err := LoadSecret(cron.cfg.AWS.SecretFilePath)
		if err != nil {
			log.WithField("error", err).Error("Failed to load secrets")
			sleep(cron.quitC, 30*time.Second)
			return nil
		}
		return secret
	}
	return nil
}
