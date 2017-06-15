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
	"flag"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/uber/cherami-server/common/configure"
)

// Config represetns the command line config
type cmdlineConfig struct {
	cmdType     string
	cfgFilePath string
}

// RunBackupTool runs the backup tool
func RunBackupTool(args []string) {

	cmdlineCfg, cfg := parseBackupToolConfig(args)
	if cmdlineCfg == nil || cfg == nil {
		return
	}

	log.Infof("Config:%v", cfg)

	switch cmdlineCfg.cmdType {
	case "cron":
		runCron(cfg)
	case "backup":
		runBackupTask(cfg)
	case "restore":
		runRestoreTask(cfg)
	}
}

func runCron(cfg *BackupToolConfig) {
	log.Infof("Starting backup cron")
	cron := NewBackupCron("cherami-cassandra", cfg)
	cron.Run()
	log.Infof("Cassandra backup cron stopped")
}

func runBackupTask(cfg *BackupToolConfig) {
	var date = time.Now().Format("20060102")
	log.Infof("Starting backup, name=%v", date)
	cron := NewBackupCron("cherami-cassandra", cfg)
	err := cron.ScheduleNow(date)
	if err != nil {
		log.Errorf("Backup failed, err=%v", err)
		return
	}
	log.Infof("Backup successful")
}

func runRestoreTask(cfg *BackupToolConfig) {
	secret, err := LoadSecret(cfg.AWS.SecretFilePath)
	if err != nil {
		log.Fatalf("Failed to load secret yaml, err=%v", err)
	}
	task, err := NewRestoreTask(cfg, secret)
	if err != nil {
		log.Fatalf("Failed to create restore task, err=%v", err)
	}
	log.Infof("Starting restore task")
	err = task.Run()
	if err != nil {
		log.Fatalf("Error restoring, err=%v", err)
	}
	log.Infof("Restore successful")
}

func parseBackupToolConfig(args []string) (*cmdlineConfig, *BackupToolConfig) {

	var cmdlineCfg cmdlineConfig
	var cfg BackupToolConfig

	fmt.Printf("BackupTool Args:%v\n", args)

	if len(args) < 1 || args[0] != "backupTool" {
		printBackupToolHelp()
		return nil, nil
	}

	cmd := flag.NewFlagSet("backupTool", flag.ExitOnError)
	cmd.StringVar(&cmdlineCfg.cmdType, "t", "cron", "Command type, must be in [backup,restore,cron]")
	cmd.StringVar(&cmdlineCfg.cfgFilePath, "f", "", "Config YAML file path")

	if err := cmd.Parse(args[1:]); err != nil {
		printBackupToolHelp()
		return nil, nil
	}

	if len(cmdlineCfg.cmdType) == 0 || len(cmdlineCfg.cfgFilePath) == 0 {
		printBackupToolHelp()
		fmt.Println("Invalid arguments")
		return nil, nil
	}

	if err := configure.NewCommonConfigure().LoadFile(&cfg, cmdlineCfg.cfgFilePath); err != nil {
		fmt.Println("Error initializing configuration:", err)
		return nil, nil
	}

	if cmdlineCfg.cmdType == "backup" || cmdlineCfg.cmdType == "cron" {
		err := ValidateBackupConfig(&cfg)
		if err != nil {
			printBackupToolHelp()
			fmt.Println("Config YAML validation failed:", err)
			return nil, nil
		}
		return &cmdlineCfg, &cfg
	}

	if cmdlineCfg.cmdType == "restore" {
		err := ValidateRestoreConfig(&cfg)
		if err != nil {
			printBackupToolHelp()
			fmt.Println("Config YAML validation failed:", err)
			return nil, nil
		}
		return &cmdlineCfg, &cfg
	}

	printBackupToolHelp()
	fmt.Println("Unknown command ", cmdlineCfg.cmdType)
	return nil, nil
}

func printBackupToolHelp() {
	helpMessage := `./backupTool -t [backup/restore/cron] -f [configYAMLFilePath]`
	fmt.Println(helpMessage)
}
