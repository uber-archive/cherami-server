package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/uber/cherami-server/storage"
	"github.com/uber/cherami-server/storage/manyrocks"
)

type arguments struct {
	store      string
	extent     string
	baseDir    string
	start      storage.Key
	end        storage.Key
	num        int
	printVal   bool
	formatTime bool
	help       bool
}

func main() {

	args := parseArgs()

	if args == nil {
		return
	}

	var db storage.ExtentStore
	var err error

	switch args.store {
	case "manyrocks":
		db, err = manyrocks.OpenExtentDB(storage.ExtentUUID(args.extent), fmt.Sprintf("%s/%s", args.baseDir, args.extent))
		if err != nil {
			fmt.Printf("error opening db (%s): %v\n", args.baseDir, err)
			return
		}

	default:
		fmt.Printf("unsupported store: %s\n", args.store)
		return
	}

	defer db.CloseExtentDB()

	dumpExtentDB(db, args)

	return
}

func dumpExtentDB(db storage.ExtentStore, args *arguments) {

	addr, key, err := db.SeekCeiling(args.start)

	if err != nil {
		fmt.Printf("db.SeekCeiling(%x) error: %v\n", args.start, err)
		return
	}

	var val storage.Value

	if args.printVal {

		_, val, _, _, err = db.Get(addr)

		if err != nil {
			fmt.Printf("db.Get(%x) errored: %v\n", addr, err)
			return
		}
	}

	var num int
	for (addr != storage.EOX) && (key < args.end) && (num < args.num) {

		// fmt.Printf("key = %d %x %v %p\n", key, key, key, key)

		if isSealExtentKey(key) {

			sealSeqNum := deconstructSealExtentKey(key)

			if sealSeqNum == seqNumUnspecifiedSeal {
				fmt.Printf("0x%016v => SEALED (seqnum: unspecified)\n", key)
			} else {
				fmt.Printf("0x%016v => SEALED (seqnum: %d)\n", key, sealSeqNum)
			}

		} else {

			ts, seqnum := deconstructKey(key)

			if args.printVal {

				var enqTime, payload, vTime string

				msg, errd := deserializeMessage(val)
				enq := msg.GetEnqueueTimeUtc()

				if errd != nil { // corrupt message?

					payload = fmt.Sprintf("ERROR deserializing data: %v (val=%v)", errd, val)

				} else {

					if args.formatTime {
						enqTime = time.Unix(0, enq).Format(time.RFC3339Nano)
					} else {
						enqTime = strconv.FormatInt(enq, 16)
					}

					payload = fmt.Sprintf("seq=%d enq=%v data=%d bytes",
						msg.GetSequenceNumber(), enqTime, len(msg.GetPayload().GetData()))
					// payload = msg.String()
				}

				if args.formatTime {
					vTime = time.Unix(0, ts).Format(time.RFC3339Nano)
				} else {
					vTime = strconv.FormatInt(ts, 16)
				}

				fmt.Printf("0x%016v => #%d ts=%v payload:[%v]\n", key, seqnum, vTime, payload)

			} else {

				fmt.Printf("0x%016v => #%d ts=%v\n", key, seqnum, ts)
			}
		}

		num++

		if args.printVal {

			if key, val, addr, _, err = db.Get(addr); err != nil {
				fmt.Printf("db.Get(%x) errored: %v\n", addr, err)
				break
			}

		} else {

			if addr, key, err = db.Next(addr); err != nil {
				fmt.Printf("db.Next(%x) errored: %v\n", addr, err)
				break
			}
		}
	}

	fmt.Printf("summary: dumped %d keys in range [%v, %v)\n", num, args.start, args.end)
	return
}

func parseArgs() (args *arguments) {

	args = &arguments{}

	flag.StringVar(&args.store, "store", "manyrocks", "store")
	flag.StringVar(&args.extent, "x", "", "extent")
	flag.StringVar(&args.baseDir, "base", ".", "base dir")

	var start, end, num string

	flag.StringVar(&start, "s", "-1", "start range")
	flag.StringVar(&end, "e", "-1", "end range")
	flag.StringVar(&num, "n", "-1", "number of values")

	flag.BoolVar(&args.printVal, "v", false, "deserialize payload")
	flag.BoolVar(&args.formatTime, "t", false, "format time")

	flag.BoolVar(&args.help, "?", false, "help")
	flag.BoolVar(&args.help, "help", false, "help")

	flag.Parse()

	switch {
	case args.extent == "":
		cwd, _ := os.Getwd()

		args.extent = filepath.Base(cwd)
		args.baseDir = filepath.Dir(cwd)

	case args.baseDir == "":
		args.baseDir, _ = os.Getwd()
	}

	switch i, err := strconv.ParseInt(start, 0, 64); {
	case err != nil:
		fmt.Printf("error parsing start arg (%s): %v\n", start, err)
		return nil
	case i < 0:
		args.start = 0
	default:
		args.start = storage.Key(i)
	}

	switch i, err := strconv.ParseInt(end, 0, 64); {
	case err != nil:
		fmt.Printf("error parsing end arg (%s): %v\n", end, err)
		return nil
	case i < 0:
		args.end = storage.Key(math.MaxInt64)
	default:
		args.end = storage.Key(i)
	}

	switch i, err := strconv.ParseInt(num, 0, 64); {
	case err != nil:
		fmt.Printf("error parsing num arg (%s): %v\n", num, err)
		return nil
	case i < 0:
		args.num = math.MaxInt64
	default:
		args.num = int(i)
	}

	// fmt.Printf("args=%v\n", args)

	return args
}
