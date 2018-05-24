package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/gocql/gocql"
)

type outputType int

const (
	outputNull outputType = iota
	outputCqlDelete
	outputCqlDeleteUndo
	outputJSON
	outputShort
)

type cmqWriter interface {
	DestinationStart()                                    // destPath, destUUID string
	Destination(row map[string]interface{}, annot string) // destPath, destUUID string
	DestinationEnd()                                      // destPath, destUUID string

	ConsumerGroupStart()
	ConsumerGroup(row map[string]interface{}, annot string) // destPath, destUUID, cgPath, cgUUID string
	ConsumerGroupEnd()

	ExtentStart()
	Extent(row map[string]interface{}, annot string) // destPath, destUUID, extUUID string
	ExtentEnd()

	ConsumerGroupExtentStart()
	ConsumerGroupExtent(row map[string]interface{}, annot string) // destUUID, cgUUID, extUUID string
	ConsumerGroupExtentEnd()

	InputExtentStart()
	InputExtent(row map[string]interface{}, annot string) // destUUID, extUUID, inputUUID string
	InputExtentEnd()

	StoreExtentStart()
	StoreExtent(row map[string]interface{}, annot string) // destUUID, extUUID, storeUUID string
	StoreExtentEnd()

	close()
}

type outContext struct {
	destPaths map[string]string // dest-uuid to dest-path
	cgPaths   map[string]string // cg-uuid to cg-path

	cgDest  map[string]string // cg-uuid to dest-uuid
	extDest map[string]string // ext-uuid to dest-uuid
}

func cmqOutputWriter(outTypes []string) (writer cmqWriter) {

	// override 'output' format, if specified
	if len(outTypes) == 0 {
		outTypes = []string{`json`}
	}

	var writers []cmqWriter

	for _, ot := range outTypes {

		switch ot {
		case "delete":
			fallthrough
		case "deletecql":
			writers = append(writers, newCmqWriterCqlDelete())

		case "undo":
			fallthrough
		case "undocql":
			writers = append(writers, newCmqWriterCqlDeleteUndo())

		case "json":
			writers = append(writers, newCmqWriterJSON())

		case "short":
			writers = append(writers, newCmqWriterShort())

		case "none":
			fallthrough
		case "null":
			writers = append(writers, newCmqWriterNull())

		default:
			fmt.Printf("unknown output-type: %d\n", ot)
		}
	}

	if len(writers) == 1 {
		return writers[0]
	}

	return newCmqWriterMulti(writers)
}

func getCmqWriter(outTypes ...outputType) (writer cmqWriter) {

	var writers []cmqWriter

	for _, ot := range outTypes {

		switch ot {
		case outputNull:
			writers = append(writers, newCmqWriterNull())

		case outputShort:
			writers = append(writers, newCmqWriterShort())

		case outputJSON:
			writers = append(writers, newCmqWriterJSON())

		case outputCqlDelete:
			writers = append(writers, newCmqWriterCqlDelete())

		case outputCqlDeleteUndo:
			writers = append(writers, newCmqWriterCqlDeleteUndo())
		}
	}

	if len(writers) == 1 {
		return writers[0]
	}

	return newCmqWriterMulti(writers)
}

type cmqWriterMulti struct {
	writers []cmqWriter
}

func newCmqWriterMulti(writers []cmqWriter) cmqWriter {
	return &cmqWriterMulti{writers: writers}
}

func (t *cmqWriterMulti) Destination(row map[string]interface{}, annot string) {
	for _, w := range t.writers {
		w.Destination(row, annot)
	}
}

func (t *cmqWriterMulti) ConsumerGroup(row map[string]interface{}, annot string) {
	for _, w := range t.writers {
		w.ConsumerGroup(row, annot)
	}
}

func (t *cmqWriterMulti) Extent(row map[string]interface{}, annot string) {
	for _, w := range t.writers {
		w.Extent(row, annot)
	}
}

func (t *cmqWriterMulti) ConsumerGroupExtent(row map[string]interface{}, annot string) {
	for _, w := range t.writers {
		w.ConsumerGroupExtent(row, annot)
	}
}

func (t *cmqWriterMulti) InputExtent(row map[string]interface{}, annot string) {
	for _, w := range t.writers {
		w.InputExtent(row, annot)
	}
}

func (t *cmqWriterMulti) StoreExtent(row map[string]interface{}, annot string) {
	for _, w := range t.writers {
		w.StoreExtent(row, annot)
	}
}

func (t *cmqWriterMulti) DestinationStart() {
	for _, w := range t.writers {
		w.DestinationStart()
	}
}

func (t *cmqWriterMulti) DestinationEnd() {
	for _, w := range t.writers {
		w.DestinationEnd()
	}
}

func (t *cmqWriterMulti) ConsumerGroupStart() {
	for _, w := range t.writers {
		w.ConsumerGroupStart()
	}
}

func (t *cmqWriterMulti) ConsumerGroupEnd() {
	for _, w := range t.writers {
		w.ConsumerGroupEnd()
	}
}

func (t *cmqWriterMulti) ExtentStart() {
	for _, w := range t.writers {
		w.ExtentStart()
	}
}

func (t *cmqWriterMulti) ExtentEnd() {
	for _, w := range t.writers {
		w.ExtentEnd()
	}
}

func (t *cmqWriterMulti) ConsumerGroupExtentStart() {
	for _, w := range t.writers {
		w.ConsumerGroupExtentStart()
	}
}

func (t *cmqWriterMulti) ConsumerGroupExtentEnd() {
	for _, w := range t.writers {
		w.ConsumerGroupExtentEnd()
	}
}

func (t *cmqWriterMulti) StoreExtentStart() {
	for _, w := range t.writers {
		w.StoreExtentStart()
	}
}

func (t *cmqWriterMulti) StoreExtentEnd() {
	for _, w := range t.writers {
		w.StoreExtentEnd()
	}
}

func (t *cmqWriterMulti) InputExtentStart() {
	for _, w := range t.writers {
		w.InputExtentStart()
	}
}

func (t *cmqWriterMulti) InputExtentEnd() {
	for _, w := range t.writers {
		w.InputExtentEnd()
	}
}

func (t *cmqWriterMulti) close() {
	for _, w := range t.writers {
		w.close()
	}
}

type cmqWriterShort struct {
}

func newCmqWriterShort() *cmqWriterShort {
	return &cmqWriterShort{}
}

func (t *cmqWriterShort) Destination(row map[string]interface{}, annot string) {
	destUUID := row["uuid"]
	destPath := row["destination"].(map[string]interface{})["path"]
	fmt.Printf("dest=%v ('%v')\n", destUUID, destPath)
}

func (t *cmqWriterShort) ConsumerGroup(row map[string]interface{}, annot string) {
	cgUUID := row["uuid"]
	cgPath := row["consumer_group"].(map[string]interface{})["name"]
	destUUID := row["consumer_group"].(map[string]interface{})["destination_uuid"]
	fmt.Printf("dest=%v cg=%v ('%v') %s\n", destUUID, cgUUID, cgPath, annot)
}

func (t *cmqWriterShort) Extent(row map[string]interface{}, annot string) {
	destUUID := row["destination_uuid"]
	extUUID := row["extent_uuid"]
	fmt.Printf("dest=%v ext=%v\n", destUUID, extUUID)
}

func (t *cmqWriterShort) ConsumerGroupExtent(row map[string]interface{}, annot string) {
	cgUUID := row["consumer_group_uuid"]
	extUUID := row["extent_uuid"]
	fmt.Printf("cg=%v ext=%v\n", cgUUID, extUUID)
}

func (t *cmqWriterShort) InputExtent(row map[string]interface{}, annot string) {
}

func (t *cmqWriterShort) StoreExtent(row map[string]interface{}, annot string) {
}

func (t *cmqWriterShort) DestinationStart() {
}

func (t *cmqWriterShort) DestinationEnd() {
}

func (t *cmqWriterShort) ConsumerGroupStart() {
}

func (t *cmqWriterShort) ConsumerGroupEnd() {
}

func (t *cmqWriterShort) ExtentStart() {
}

func (t *cmqWriterShort) ExtentEnd() {
}

func (t *cmqWriterShort) ConsumerGroupExtentStart() {
}

func (t *cmqWriterShort) ConsumerGroupExtentEnd() {
}

func (t *cmqWriterShort) StoreExtentStart() {
}

func (t *cmqWriterShort) StoreExtentEnd() {
}

func (t *cmqWriterShort) InputExtentStart() {
}

func (t *cmqWriterShort) InputExtentEnd() {
}

func (t *cmqWriterShort) close() {
}

type cmqWriterCqlDelete struct {
	cql *os.File
}

func newCmqWriterCqlDelete() cmqWriter {

	prefix := time.Now().Format("20060102T150405_")

	t := &cmqWriterCqlDelete{}

	var err error

	if t.cql, err = os.Create(prefix + "delete.cql"); err != nil {
		fmt.Printf("error creating file: %v\n", prefix+"delete.cql")
	}

	fmt.Fprintf(t.cql, "CONSISTENCY ALL;\n")
	return t
}

func (t *cmqWriterCqlDelete) Destination(row map[string]interface{}, annot string) {

	destUUID := row["uuid"].(gocql.UUID).String()

	fmt.Fprintf(t.cql, "DELETE FROM destinations WHERE uuid=%v; -- %s\n", destUUID, annot)
}

func (t *cmqWriterCqlDelete) ConsumerGroup(row map[string]interface{}, annot string) {

	cgUUID := row["uuid"].(gocql.UUID).String()

	fmt.Fprintf(t.cql, "DELETE FROM consumer_groups WHERE uuid=%v; -- %s\n", cgUUID, annot)
}

func (t *cmqWriterCqlDelete) Extent(row map[string]interface{}, annot string) {

	extUUID := row["extent_uuid"].(gocql.UUID).String()
	destUUID := row["destination_uuid"].(gocql.UUID).String()

	fmt.Fprintf(t.cql, "DELETE FROM destination_extents WHERE destination_uuid=%v AND extent_uuid=%v; -- %s\n", destUUID, extUUID, annot)
}

func (t *cmqWriterCqlDelete) ConsumerGroupExtent(row map[string]interface{}, annot string) {

	cgUUID := row["consumer_group_uuid"].(gocql.UUID).String()
	extUUID := row["extent_uuid"].(gocql.UUID).String()

	fmt.Fprintf(t.cql, "DELETE FROM consumer_group_extents WHERE consumer_group_uuid=%v AND extent_uuid=%v; -- %s\n", cgUUID, extUUID, annot)
}

func (t *cmqWriterCqlDelete) InputExtent(row map[string]interface{}, annot string) {

	destUUID := row["destination_uuid"].(gocql.UUID).String()
	extUUID := row["extent_uuid"].(gocql.UUID).String()
	inputUUID := row["input_host_uuid"].(gocql.UUID).String()

	fmt.Fprintf(t.cql, "DELETE FROM input_host_extents WHERE destination_uuid=%v AND extent_uuid=%v AND input_host_uuid=%v; -- %s\n",
		destUUID, extUUID, inputUUID, annot)
}

func (t *cmqWriterCqlDelete) StoreExtent(row map[string]interface{}, annot string) {

	extUUID := row["extent_uuid"].(gocql.UUID).String()
	storeUUID := row["store_uuid"].(gocql.UUID).String()

	fmt.Fprintf(t.cql, "DELETE FROM store_extents WHERE extent_uuid=%v AND store_uuid=%v; -- %s\n", extUUID, storeUUID, annot)
}

func (t *cmqWriterCqlDelete) DestinationStart() {
}

func (t *cmqWriterCqlDelete) DestinationEnd() {
}

func (t *cmqWriterCqlDelete) ConsumerGroupStart() {
}

func (t *cmqWriterCqlDelete) ConsumerGroupEnd() {
}

func (t *cmqWriterCqlDelete) ExtentStart() {
}

func (t *cmqWriterCqlDelete) ExtentEnd() {
}

func (t *cmqWriterCqlDelete) ConsumerGroupExtentStart() {
}

func (t *cmqWriterCqlDelete) ConsumerGroupExtentEnd() {
}

func (t *cmqWriterCqlDelete) StoreExtentStart() {
}

func (t *cmqWriterCqlDelete) StoreExtentEnd() {
}

func (t *cmqWriterCqlDelete) InputExtentStart() {
}

func (t *cmqWriterCqlDelete) InputExtentEnd() {
}

func (t *cmqWriterCqlDelete) close() {
	t.cql.Close()
}

type cmqWriterCqlDeleteUndo struct {
	cql *os.File
}

func newCmqWriterCqlDeleteUndo() cmqWriter {

	prefix := time.Now().Format("20060102T150405_")

	t := &cmqWriterCqlDeleteUndo{}

	var err error

	if t.cql, err = os.Create(prefix + "undo.cql"); err != nil {
		fmt.Printf("error creating file: %v\n", prefix+"undo.cql")
	}

	fmt.Fprintf(t.cql, "CONSISTENCY ALL;\n")
	return t
}

func (t *cmqWriterCqlDeleteUndo) Destination(row map[string]interface{}, annot string) {
	if j, err := json.Marshal(row); err == nil {
		fmt.Fprintf(t.cql, "INSERT INTO destinations JSON '%s'; -- %s\n", string(j), annot)
	} else {
		fmt.Printf("cmqWriterCqlDeleteUndo.Destination: json.Marshal error: %v\n", err)
	}
}

func (t *cmqWriterCqlDeleteUndo) ConsumerGroup(row map[string]interface{}, annot string) {
	if j, err := json.Marshal(row); err == nil {
		fmt.Fprintf(t.cql, "INSERT INTO consumer_groups JSON '%s'; -- %s\n", string(j), annot)
	} else {
		fmt.Printf("cmqWriterCqlDeleteUndo.ConsumerGroup: json.Marshal error: %v\n", err)
	}
}

func (t *cmqWriterCqlDeleteUndo) Extent(row map[string]interface{}, annot string) {
	if j, err := json.Marshal(row); err == nil {
		fmt.Fprintf(t.cql, "INSERT INTO destination_extents JSON '%s'; -- %s\n", string(j), annot)
	} else {
		fmt.Printf("cmqWriterCqlDeleteUndo.Extent: json.Marshal error: %v\n", err)
	}
}

func (t *cmqWriterCqlDeleteUndo) ConsumerGroupExtent(row map[string]interface{}, annot string) {
	if j, err := json.Marshal(row); err == nil {
		fmt.Fprintf(t.cql, "INSERT INTO consumer_group_extents JSON '%s'; -- %s\n", string(j), annot)
	} else {
		fmt.Printf("cmqWriterCqlDeleteUndo.ConsumerGroupExtent: json.Marshal error: %v\n", err)
	}
}

func (t *cmqWriterCqlDeleteUndo) InputExtent(row map[string]interface{}, annot string) {
	if j, err := json.Marshal(row); err == nil {
		fmt.Fprintf(t.cql, "INSERT INTO input_host_extents JSON '%s'; -- %s\n", string(j), annot)
	} else {
		fmt.Printf("cmqWriterCqlDeleteUndo.InputExtent: json.Marshal error: %v\n", err)
	}
}

func (t *cmqWriterCqlDeleteUndo) StoreExtent(row map[string]interface{}, annot string) {
	if j, err := json.Marshal(row); err == nil {
		fmt.Fprintf(t.cql, "INSERT INTO store_extents JSON '%s';\n", string(j), annot)
	} else {
		fmt.Printf("cmqWriterCqlDeleteUndo.StoreExtent: json.Marshal error: %v\n", err)
	}
}

func (t *cmqWriterCqlDeleteUndo) DestinationStart() {
}

func (t *cmqWriterCqlDeleteUndo) DestinationEnd() {
}

func (t *cmqWriterCqlDeleteUndo) ConsumerGroupStart() {
}

func (t *cmqWriterCqlDeleteUndo) ConsumerGroupEnd() {
}

func (t *cmqWriterCqlDeleteUndo) ExtentStart() {
}

func (t *cmqWriterCqlDeleteUndo) ExtentEnd() {
}

func (t *cmqWriterCqlDeleteUndo) ConsumerGroupExtentStart() {
}

func (t *cmqWriterCqlDeleteUndo) ConsumerGroupExtentEnd() {
}

func (t *cmqWriterCqlDeleteUndo) StoreExtentStart() {
}

func (t *cmqWriterCqlDeleteUndo) StoreExtentEnd() {
}

func (t *cmqWriterCqlDeleteUndo) InputExtentStart() {
}

func (t *cmqWriterCqlDeleteUndo) InputExtentEnd() {
}

func (t *cmqWriterCqlDeleteUndo) close() {

	t.cql.Close()
}

type cmqWriterJSON struct {
	dRow, cgRow, xRow, cgxRow, ixRow, sxRow int
}

func newCmqWriterJSON() *cmqWriterJSON {
	return &cmqWriterJSON{}
}

func (t *cmqWriterJSON) DestinationStart() {
	fmt.Println("{")
}

func (t *cmqWriterJSON) DestinationEnd() {
	fmt.Println("\n}")
}

func (t *cmqWriterJSON) Destination(row map[string]interface{}, annot string) {

	if t.dRow > 0 {
		fmt.Printf(",\n")
	}

	out, _ := json.MarshalIndent(row, "        ", "            ")
	fmt.Printf("        \"%v\": %v", row["uuid"].(gocql.UUID).String(), string(out))

	t.dRow++
}

func (t *cmqWriterJSON) ConsumerGroupStart() {
	fmt.Println("{")
}

func (t *cmqWriterJSON) ConsumerGroupEnd() {
	fmt.Println("\n}")
}

func (t *cmqWriterJSON) ConsumerGroup(row map[string]interface{}, annot string) {

	if t.cgRow > 0 {
		fmt.Printf(",\n")
	}

	out, _ := json.MarshalIndent(row, "        ", "            ")
	fmt.Printf("        \"%v\": %v", row["uuid"], string(out))

	t.cgRow++
}

func (t *cmqWriterJSON) ExtentStart() {
	fmt.Println("{")
}

func (t *cmqWriterJSON) ExtentEnd() {
	fmt.Println("\n}")
}

func (t *cmqWriterJSON) Extent(row map[string]interface{}, annot string) {

	if t.xRow > 0 {
		fmt.Printf(",\n")
	}

	out, _ := json.MarshalIndent(row, "        ", "            ")
	fmt.Printf("        \"%v\": %v", row["extent_uuid"], string(out))

	t.xRow++
}

func (t *cmqWriterJSON) ConsumerGroupExtentStart() {
	fmt.Println("{")
}

func (t *cmqWriterJSON) ConsumerGroupExtentEnd() {
	fmt.Println("\n}")
}

func (t *cmqWriterJSON) ConsumerGroupExtent(row map[string]interface{}, annot string) {

	if t.cgxRow > 0 {
		fmt.Printf(",\n")
	}

	out, _ := json.MarshalIndent(row, "        ", "            ")
	fmt.Printf("        \"{%v: %v}\": %v", row["consumer_group_uuid"], row["extent_uuid"], string(out))

	t.cgxRow++
}

func (t *cmqWriterJSON) InputExtentStart() {
	fmt.Println("{")
}

func (t *cmqWriterJSON) InputExtentEnd() {
	fmt.Println("\n}")
}

func (t *cmqWriterJSON) InputExtent(row map[string]interface{}, annot string) {

	if t.ixRow > 0 {
		fmt.Printf(",\n")
	}

	out, _ := json.MarshalIndent(row, "        ", "            ")
	fmt.Printf("        \"{%v: %v}\": %v", row["input_host_uuid"], row["extent_uuid"], string(out))

	t.ixRow++
}

func (t *cmqWriterJSON) StoreExtentStart() {
	fmt.Println("{")
}

func (t *cmqWriterJSON) StoreExtentEnd() {
	fmt.Println("\n}")
}

func (t *cmqWriterJSON) StoreExtent(row map[string]interface{}, annot string) {

	if t.sxRow > 0 {
		fmt.Printf(",\n")
	}

	out, _ := json.MarshalIndent(row, "        ", "            ")
	fmt.Printf("        \"{%v: %v}\": %v", row["store_uuid"], row["extent_uuid"], string(out))

	t.sxRow++
}

func (t *cmqWriterJSON) close() {
}

type cmqWriterNull struct{}

func newCmqWriterNull() *cmqWriterNull {
	return &cmqWriterNull{}
}

func (t *cmqWriterNull) Destination(row map[string]interface{}, annot string) {
}

func (t *cmqWriterNull) ConsumerGroup(row map[string]interface{}, annot string) {
}

func (t *cmqWriterNull) Extent(row map[string]interface{}, annot string) {
}

func (t *cmqWriterNull) ConsumerGroupExtent(row map[string]interface{}, annot string) {
}

func (t *cmqWriterNull) InputExtent(row map[string]interface{}, annot string) {
}

func (t *cmqWriterNull) StoreExtent(row map[string]interface{}, annot string) {
}

func (t *cmqWriterNull) DestinationStart() {
}

func (t *cmqWriterNull) DestinationEnd() {
}

func (t *cmqWriterNull) ConsumerGroupStart() {
}

func (t *cmqWriterNull) ConsumerGroupEnd() {
}

func (t *cmqWriterNull) ExtentStart() {
}

func (t *cmqWriterNull) ExtentEnd() {
}

func (t *cmqWriterNull) ConsumerGroupExtentStart() {
}

func (t *cmqWriterNull) ConsumerGroupExtentEnd() {
}

func (t *cmqWriterNull) StoreExtentStart() {
}

func (t *cmqWriterNull) StoreExtentEnd() {
}

func (t *cmqWriterNull) InputExtentStart() {
}

func (t *cmqWriterNull) InputExtentEnd() {
}

func (t *cmqWriterNull) close() {
}
