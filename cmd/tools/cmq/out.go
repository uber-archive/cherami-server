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
	Destination(row map[string]interface{}, annot string)         // destPath, destUUID string
	ConsumerGroup(row map[string]interface{}, annot string)       // destPath, destUUID, cgPath, cgUUID string
	Extent(row map[string]interface{}, annot string)              // destPath, destUUID, extUUID string
	ConsumerGroupExtent(row map[string]interface{}, annot string) // destUUID, cgUUID, extUUID string
	InputExtent(row map[string]interface{}, annot string)         // destUUID, extUUID, inputUUID string
	StoreExtent(row map[string]interface{}, annot string)         // destUUID, extUUID, storeUUID string
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
	fmt.Printf("destination: %v [%v]\n", destPath, destUUID)
}

func (t *cmqWriterShort) ConsumerGroup(row map[string]interface{}, annot string) {
	cgUUID := row["uuid"]
	cgPath := row["consumer_group"].(map[string]interface{})["name"]
	destUUID := row["destination_uuid"]
	fmt.Printf("consumer_group: dest=%v, cg='%v' [%v]\n", destUUID, cgPath, cgUUID)
}

func (t *cmqWriterShort) Extent(row map[string]interface{}, annot string) {
	destUUID := row["destination_uuid"]
	extUUID := row["extent_uuid"]
	fmt.Printf("extent: dest=%v, ext=%v\n", destUUID, extUUID)
}

func (t *cmqWriterShort) ConsumerGroupExtent(row map[string]interface{}, annot string) {
	cgUUID := row["consumer_group_uuid"]
	extUUID := row["extent_uuid"]
	fmt.Printf("consumer_group_extent: cg=%v ext=%v\n", cgUUID, extUUID)
}

func (t *cmqWriterShort) InputExtent(row map[string]interface{}, annot string) {
}

func (t *cmqWriterShort) StoreExtent(row map[string]interface{}, annot string) {
}

func (t *cmqWriterShort) close() {
}

type cmqWriterCqlDelete struct {
	fD, fCG, fDE, fCGX, fIX, fSX *os.File
}

func newCmqWriterCqlDelete() cmqWriter {

	prefix := time.Now().Format("20060102T150405_")

	t := &cmqWriterCqlDelete{}

	var err error

	if t.fD, err = os.Create(prefix + "destinations.cql"); err != nil {
		fmt.Printf("error creating file: %v\n", prefix+"destinations.cql")
	}

	if t.fCG, err = os.Create(prefix + "consumer_groups.cql"); err != nil {
		fmt.Printf("error creating file: %v\n", prefix+"consumer_groups.cql")
	}

	if t.fDE, err = os.Create(prefix + "destination_extents.cql"); err != nil {
		fmt.Printf("error creating file: %v\n", prefix+"destination_extents.cql")
	}

	if t.fCGX, err = os.Create(prefix + "consumer_group_extents.cql"); err != nil {
		fmt.Printf("error creating file: %v\n", prefix+"consumer_group_extents.cql")
	}

	if t.fIX, err = os.Create(prefix + "input_host_extents.cql"); err != nil {
		fmt.Printf("error creating file: %v\n", prefix+"input_host_extents.cql")
	}

	if t.fSX, err = os.Create(prefix + "store_extents.cql"); err != nil {
		fmt.Printf("error creating file: %v\n", prefix+"store_extents.cql")
	}

	return t
}

func (t *cmqWriterCqlDelete) Destination(row map[string]interface{}, annot string) {
	destUUID := row["uuid"].(gocql.UUID).String()
	fmt.Fprintf(t.fD, "DELETE FROM destinations WHERE uuid=%v; -- %s\n", destUUID, annot)
}

func (t *cmqWriterCqlDelete) ConsumerGroup(row map[string]interface{}, annot string) {
	cgUUID := row["uuid"].(gocql.UUID).String()
	fmt.Fprintf(t.fCG, "DELETE FROM consumer_groups WHERE uuid=%v; -- %s\n", cgUUID, annot)
}

func (t *cmqWriterCqlDelete) Extent(row map[string]interface{}, annot string) {
	extUUID := row["extent_uuid"].(gocql.UUID).String()
	destUUID := row["destination_uuid"].(gocql.UUID).String()
	fmt.Fprintf(t.fDE, "DELETE FROM destination_extents WHERE destination_uuid=%v AND extent_uuid=%v; -- %s\n", destUUID, extUUID, annot)
}

func (t *cmqWriterCqlDelete) ConsumerGroupExtent(row map[string]interface{}, annot string) {
	cgUUID := row["consumer_group_uuid"].(gocql.UUID).String()
	extUUID := row["extent_uuid"].(gocql.UUID).String()
	fmt.Fprintf(t.fCGX, "DELETE FROM consumer_group_extents WHERE consumer_group_uuid=%v AND extent_uuid=%v; -- %s\n", cgUUID, extUUID, annot)
}

func (t *cmqWriterCqlDelete) InputExtent(row map[string]interface{}, annot string) {
	destUUID := row["destination_uuid"].(gocql.UUID).String()
	extUUID := row["extent_uuid"].(gocql.UUID).String()
	inputUUID := row["input_host_uuid"].(gocql.UUID).String()
	fmt.Fprintf(t.fIX, "DELETE FROM input_host_extents WHERE destination_uuid=%v AND extent_uuid=%v AND input_host_uuid=%v; -- %s\n",
		destUUID, extUUID, inputUUID, annot)
}

func (t *cmqWriterCqlDelete) StoreExtent(row map[string]interface{}, annot string) {
	extUUID := row["extent_uuid"].(gocql.UUID).String()
	storeUUID := row["store_uuid"].(gocql.UUID).String()
	fmt.Fprintf(t.fSX, "DELETE FROM store_extents WHERE extent_uuid=%v AND store_uuid=%v; -- %s\n", extUUID, storeUUID, annot)
}

func (t *cmqWriterCqlDelete) close() {
	t.fSX.Close()
	t.fIX.Close()
	t.fCGX.Close()
	t.fCG.Close()
	t.fDE.Close()
	t.fD.Close()
}

type cmqWriterCqlDeleteUndo struct {
	fD, fCG, fDE, fCGX, fIX, fSX *os.File
}

func newCmqWriterCqlDeleteUndo() cmqWriter {

	prefix := time.Now().Format("20060102T150405_")

	t := &cmqWriterCqlDeleteUndo{}

	var err error

	if t.fD, err = os.Create(prefix + "destinations.undo"); err != nil {
		fmt.Printf("error creating file: %v\n", prefix+"destinations.undo")
	}

	if t.fCG, err = os.Create(prefix + "consumer_groups.undo"); err != nil {
		fmt.Printf("error creating file: %v\n", prefix+"consumer_groups.undo")
	}

	if t.fDE, err = os.Create(prefix + "destination_extents.undo"); err != nil {
		fmt.Printf("error creating file: %v\n", prefix+"destination_extents.undo")
	}

	if t.fCGX, err = os.Create(prefix + "consumer_group_extents.undo"); err != nil {
		fmt.Printf("error creating file: %v\n", prefix+"consumer_group_extents.undo")
	}

	if t.fIX, err = os.Create(prefix + "input_host_extents.undo"); err != nil {
		fmt.Printf("error creating file: %v\n", prefix+"input_host_extents.undo")
	}

	if t.fSX, err = os.Create(prefix + "store_extents.undo"); err != nil {
		fmt.Printf("error creating file: %v\n", prefix+"store_extents.undo")
	}

	return t
}

func (t *cmqWriterCqlDeleteUndo) Destination(row map[string]interface{}, annot string) {
	if j, err := json.Marshal(row); err == nil {
		fmt.Fprintf(t.fD, "INSERT INTO destinations JSON '%s'; -- %s\n", string(j), annot)
	} else {
		fmt.Printf("cmqWriterCqlDeleteUndo.Destination: json.Marshal error: %v\n", err)
	}
}

func (t *cmqWriterCqlDeleteUndo) ConsumerGroup(row map[string]interface{}, annot string) {
	if j, err := json.Marshal(row); err == nil {
		fmt.Fprintf(t.fCG, "INSERT INTO consumer_groups JSON '%s'; -- %s\n", string(j), annot)
	} else {
		fmt.Printf("cmqWriterCqlDeleteUndo.ConsumerGroup: json.Marshal error: %v\n", err)
	}
}

func (t *cmqWriterCqlDeleteUndo) Extent(row map[string]interface{}, annot string) {
	if j, err := json.Marshal(row); err == nil {
		fmt.Fprintf(t.fDE, "INSERT INTO destination_extents JSON '%s'; -- %s\n", string(j), annot)
	} else {
		fmt.Printf("cmqWriterCqlDeleteUndo.Extent: json.Marshal error: %v\n", err)
	}
}

func (t *cmqWriterCqlDeleteUndo) ConsumerGroupExtent(row map[string]interface{}, annot string) {
	if j, err := json.Marshal(row); err == nil {
		fmt.Fprintf(t.fCGX, "INSERT INTO consumer_group_extents JSON '%s'; -- %s\n", string(j), annot)
	} else {
		fmt.Printf("cmqWriterCqlDeleteUndo.ConsumerGroupExtent: json.Marshal error: %v\n", err)
	}
}

func (t *cmqWriterCqlDeleteUndo) InputExtent(row map[string]interface{}, annot string) {
	if j, err := json.Marshal(row); err == nil {
		fmt.Fprintf(t.fIX, "INSERT INTO input_host_extents JSON '%s'; -- %s\n", string(j), annot)
	} else {
		fmt.Printf("cmqWriterCqlDeleteUndo.InputExtent: json.Marshal error: %v\n", err)
	}
}

func (t *cmqWriterCqlDeleteUndo) StoreExtent(row map[string]interface{}, annot string) {
	if j, err := json.Marshal(row); err == nil {
		fmt.Fprintf(t.fSX, "INSERT INTO store_extents JSON '%s';\n", string(j), annot)
	} else {
		fmt.Printf("cmqWriterCqlDeleteUndo.StoreExtent: json.Marshal error: %v\n", err)
	}
}

func (t *cmqWriterCqlDeleteUndo) close() {
	t.fSX.Close()
	t.fIX.Close()
	t.fCGX.Close()
	t.fCG.Close()
	t.fDE.Close()
	t.fD.Close()
}

type cmqWriterJSON struct {
	dRow, cgRow, xRow, cgxRow, ixRow, sxRow int
}

func newCmqWriterJSON() *cmqWriterJSON {
	return &cmqWriterJSON{}
}

func (t *cmqWriterJSON) Destination(row map[string]interface{}, annot string) {

	if row == nil { // indicates start/end of a list
		if t.dRow == 0 {
			fmt.Printf("[")
			t.dRow++
		} else {
			fmt.Printf("]")
		}
		return
	}

	if t.dRow > 1 {
		fmt.Printf(",")
	}

	out, _ := json.MarshalIndent(row, "", "        ")
	fmt.Printf("%v", string(out))

	t.dRow++
}

func (t *cmqWriterJSON) ConsumerGroup(row map[string]interface{}, annot string) {

	if row == nil { // indicates start/end of a list
		if t.cgRow == 0 {
			fmt.Printf("{\n")
			t.cgRow++
		} else {
			fmt.Printf("\n}")
		}
		return
	}

	if t.cgRow > 1 {
		fmt.Printf(",\n")
	}

	out, _ := json.MarshalIndent(row, "", "        ")
	fmt.Printf("\"%v\": %v", row["uuid"].(gocql.UUID).String(), string(out))

	t.cgRow++
}

func (t *cmqWriterJSON) Extent(row map[string]interface{}, annot string) {

	if row == nil { // indicates start/end of a list
		if t.xRow == 0 {
			fmt.Printf("[")
			t.xRow++
		} else {
			fmt.Printf("]")
		}
		return
	}

	if t.xRow > 1 {
		fmt.Printf(",")
	}

	out, _ := json.MarshalIndent(row, "", "        ")
	fmt.Printf("%v", string(out))

	t.xRow++
}

func (t *cmqWriterJSON) ConsumerGroupExtent(row map[string]interface{}, annot string) {

	if row == nil { // indicates start/end of a list
		if t.cgxRow == 0 {
			fmt.Printf("[")
			t.cgxRow++
		} else {
			fmt.Printf("]")
		}
		return
	}

	if t.cgxRow > 1 {
		fmt.Printf(",")
	}

	out, _ := json.MarshalIndent(row, "", "        ")
	fmt.Printf("%v", string(out))

	t.cgxRow++
}

func (t *cmqWriterJSON) InputExtent(row map[string]interface{}, annot string) {

	if row == nil { // indicates start/end of a list
		if t.ixRow == 0 {
			fmt.Printf("[")
			t.ixRow++
		} else {
			fmt.Printf("]")
		}
		return
	}

	if t.ixRow > 1 {
		fmt.Printf(",")
	}

	out, _ := json.MarshalIndent(row, "", "        ")
	fmt.Printf("%v", string(out))

	t.ixRow++
}

func (t *cmqWriterJSON) StoreExtent(row map[string]interface{}, annot string) {

	if row == nil { // indicates start/end of a list
		if t.sxRow == 0 {
			fmt.Printf("[")
			t.sxRow++
		} else {
			fmt.Printf("]\n")
		}
		return
	}

	if t.sxRow > 1 {
		fmt.Printf(",")
	}

	out, _ := json.MarshalIndent(row, "", "        ")
	fmt.Printf("%v", string(out))

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

func (t *cmqWriterNull) close() {
}
