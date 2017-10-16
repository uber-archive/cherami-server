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
	outputJson
	outputText
)

type cmqWriter interface {
	destination(row map[string]interface{})           // destPath, destUUID string
	consumer_group(row map[string]interface{})        // destPath, destUUID, cgPath, cgUUID string
	extent(row map[string]interface{})                // destPath, destUUID, extUUID string
	consumer_group_extent(row map[string]interface{}) // destUUID, cgUUID, extUUID string
	input_extent(row map[string]interface{})          // destUUID, extUUID, inputUUID string
	store_extent(row map[string]interface{})          // destUUID, extUUID, storeUUID string
	close()
}

type outContext struct {
	destPaths map[string]string // dest-uuid to dest-path
	cgPaths   map[string]string // cg-uuid to cg-path

	cgDest  map[string]string // cg-uuid to dest-uuid
	extDest map[string]string // ext-uuid to dest-uuid
}

func getCmqWriter(ctx *outContext, outTypes ...outputType) (writer cmqWriter) {

	var writers []cmqWriter

	for _, ot := range outTypes {

		switch ot {
		case outputNull:
			writers = append(writers, newCmqWriterNull(ctx))

		case outputText:
			writers = append(writers, newCmqWriterText(ctx))

		case outputJson:
			writers = append(writers, newCmqWriterJson(ctx))

		case outputCqlDelete:
			writers = append(writers, newCmqWriterCqlDelete(ctx))

		case outputCqlDeleteUndo:
			writers = append(writers, newCmqWriterCqlDeleteUndo(ctx))
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

func (t *cmqWriterMulti) destination(row map[string]interface{}) {
	for _, w := range t.writers {
		w.destination(row)
	}
}

func (t *cmqWriterMulti) consumer_group(row map[string]interface{}) {
	for _, w := range t.writers {
		w.consumer_group(row)
	}
}

func (t *cmqWriterMulti) extent(row map[string]interface{}) {
	for _, w := range t.writers {
		w.extent(row)
	}
}

func (t *cmqWriterMulti) consumer_group_extent(row map[string]interface{}) {
	for _, w := range t.writers {
		w.consumer_group_extent(row)
	}
}

func (t *cmqWriterMulti) input_extent(row map[string]interface{}) {
	for _, w := range t.writers {
		w.input_extent(row)
	}
}

func (t *cmqWriterMulti) store_extent(row map[string]interface{}) {
	for _, w := range t.writers {
		w.store_extent(row)
	}
}

func (t *cmqWriterMulti) close() {
	for _, w := range t.writers {
		w.close()
	}
}

type cmqWriterText struct {
	ctx *outContext
}

func newCmqWriterText(ctx *outContext) *cmqWriterText {
	return &cmqWriterText{ctx: ctx}
}

func (t *cmqWriterText) destination(row map[string]interface{}) {
	destUUID := row["uuid"]
	destPath := row["destination"].(map[string]interface{})["path"]
	fmt.Printf("destination: %v [%v]\n", destPath, destUUID)
}

func (t *cmqWriterText) consumer_group(row map[string]interface{}) {
	cgUUID := row["uuid"]
	cgPath := row["name"]
	destUUID := row["destination_uuid"]
	fmt.Printf("consumer_group: dest=%v, cg='%v' [%v]\n", destUUID, cgPath, cgUUID)
}

func (t *cmqWriterText) extent(row map[string]interface{}) {
	destUUID := row["destination_uuid"]
	extUUID := row["extent_uuid"]
	fmt.Printf("extent: dest=%v, ext=%v\n", destUUID, extUUID)
}

func (t *cmqWriterText) consumer_group_extent(row map[string]interface{}) {
	cgUUID := row["consumer_group_uuid"]
	extUUID := row["extent_uuid"]
	fmt.Printf("consumer_group_extent: cg=%v ext=%v\n", cgUUID, extUUID)
}

func (t *cmqWriterText) input_extent(row map[string]interface{}) {
}

func (t *cmqWriterText) store_extent(row map[string]interface{}) {
}

func (t *cmqWriterText) close() {
}

type cmqWriterCqlDelete struct {
	fD, fCG, fDE, fCGX, fIX, fSX *os.File
	ctx                          *outContext
}

func newCmqWriterCqlDelete(ctx *outContext) cmqWriter {

	prefix := time.Now().Format("20060102T150405_")

	t := &cmqWriterCqlDelete{ctx: ctx}

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

func (t *cmqWriterCqlDelete) destination(row map[string]interface{}) {
	destUUID := row["uuid"].(gocql.UUID).String()
	fmt.Fprintf(t.fD, "DELETE FROM destinations WHERE uuid=%v; -- %s\n", destUUID, t.ctx.destPaths[destUUID])
}

func (t *cmqWriterCqlDelete) consumer_group(row map[string]interface{}) {
	cgUUID := row["uuid"].(gocql.UUID).String()
	fmt.Fprintf(t.fCG, "DELETE FROM consumer_groups WHERE uuid=%v; -- %s\n", cgUUID, t.ctx.cgPaths[cgUUID])
}

func (t *cmqWriterCqlDelete) extent(row map[string]interface{}) {
	extUUID := row["extent_uuid"].(gocql.UUID).String()
	destUUID := row["destination_uuid"].(gocql.UUID).String()
	fmt.Fprintf(t.fDE, "DELETE FROM destination_extents WHERE destination_uuid=%v AND extent_uuid=%v; -- %s\n", destUUID, extUUID, t.ctx.destPaths[destUUID])
}

func (t *cmqWriterCqlDelete) consumer_group_extent(row map[string]interface{}) {
	cgUUID := row["consumer_group_uuid"].(gocql.UUID).String()
	extUUID := row["extent_uuid"].(gocql.UUID).String()
	fmt.Fprintf(t.fCGX, "DELETE FROM consumer_group_extents WHERE consumer_group_uuid=%v AND extent_uuid=%v; -- %s\n", cgUUID, extUUID, t.ctx.cgPaths[cgUUID])
}

func (t *cmqWriterCqlDelete) input_extent(row map[string]interface{}) {
	destUUID := row["destination_uuid"].(gocql.UUID).String()
	extUUID := row["extent_uuid"].(gocql.UUID).String()
	inputUUID := row["input_host_uuid"].(gocql.UUID).String()
	fmt.Fprintf(t.fIX, "DELETE FROM input_host_extents WHERE destination_uuid=%v AND extent_uuid=%v AND input_host_uuid=%v; -- %s\n",
		destUUID, extUUID, inputUUID, t.ctx.destPaths[destUUID])
}

func (t *cmqWriterCqlDelete) store_extent(row map[string]interface{}) {
	extUUID := row["extent_uuid"].(gocql.UUID).String()
	storeUUID := row["store_uuid"].(gocql.UUID).String()
	fmt.Fprintf(t.fSX, "DELETE FROM store_extents WHERE extent_uuid=%v AND store_uuid=%v; -- %s\n", extUUID, storeUUID, t.ctx.destPaths[t.ctx.extDest[extUUID]])
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
	ctx                          *outContext
}

func newCmqWriterCqlDeleteUndo(ctx *outContext) cmqWriter {

	prefix := time.Now().Format("20060102T150405_")

	t := &cmqWriterCqlDeleteUndo{ctx: ctx}

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

func (t *cmqWriterCqlDeleteUndo) destination(row map[string]interface{}) {
	if j, err := json.Marshal(row); err == nil {
		fmt.Fprintf(t.fD, "INSERT INTO destinations JSON '%s'; -- %s\n", string(j), t.ctx.destPaths[row["uuid"].(gocql.UUID).String()])
	} else {
		fmt.Printf("cmqWriterCqlDeleteUndo.destination: json.Marshal error: %v\n", err)
	}
}

func (t *cmqWriterCqlDeleteUndo) consumer_group(row map[string]interface{}) {
	if j, err := json.Marshal(row); err == nil {
		fmt.Fprintf(t.fCG, "INSERT INTO consumer_groups JSON '%s'; -- %s\n", string(j),
			t.ctx.cgPaths[row["uuid"].(gocql.UUID).String()])
	} else {
		fmt.Printf("cmqWriterCqlDeleteUndo.consumer_group: json.Marshal error: %v\n", err)
	}
}

func (t *cmqWriterCqlDeleteUndo) extent(row map[string]interface{}) {
	if j, err := json.Marshal(row); err == nil {
		fmt.Fprintf(t.fDE, "INSERT INTO destination_extents JSON '%s'; -- %s\n", string(j),
			t.ctx.extDest[row["extent_uuid"].(gocql.UUID).String()])
	} else {
		fmt.Printf("cmqWriterCqlDeleteUndo.extent: json.Marshal error: %v\n", err)
	}
}

func (t *cmqWriterCqlDeleteUndo) consumer_group_extent(row map[string]interface{}) {
	if j, err := json.Marshal(row); err == nil {
		fmt.Fprintf(t.fCGX, "INSERT INTO consumer_group_extents JSON '%s'; -- %s\n", string(j),
			t.ctx.cgPaths[row["consumer_group_uuid"].(gocql.UUID).String()])
	} else {
		fmt.Printf("cmqWriterCqlDeleteUndo.consumer_group_extent: json.Marshal error: %v\n", err)
	}
}

func (t *cmqWriterCqlDeleteUndo) input_extent(row map[string]interface{}) {
	if j, err := json.Marshal(row); err == nil {
		fmt.Fprintf(t.fIX, "INSERT INTO input_host_extents JSON '%s'; -- %s\n", string(j),
			t.ctx.destPaths[t.ctx.extDest[row["extent_uuid"].(gocql.UUID).String()]])
	} else {
		fmt.Printf("cmqWriterCqlDeleteUndo.input_extent: json.Marshal error: %v\n", err)
	}
}

func (t *cmqWriterCqlDeleteUndo) store_extent(row map[string]interface{}) {
	if j, err := json.Marshal(row); err == nil {
		fmt.Fprintf(t.fSX, "INSERT INTO store_extents JSON '%s';\n", string(j),
			t.ctx.destPaths[t.ctx.extDest[row["extent_uuid"].(gocql.UUID).String()]])
	} else {
		fmt.Printf("cmqWriterCqlDeleteUndo.store_extent: json.Marshal error: %v\n", err)
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

type cmqWriterJson struct {
	ctx *outContext
}

func newCmqWriterJson(ctx *outContext) *cmqWriterJson {
	fmt.Printf("cmqWriterJson: NOT IMPLEMENTED\n")
	return nil
}

func (t *cmqWriterJson) destination(row map[string]interface{}) {
}

func (t *cmqWriterJson) consumer_group(row map[string]interface{}) {
}

func (t *cmqWriterJson) extent(row map[string]interface{}) {
}

func (t *cmqWriterJson) consumer_group_extent(row map[string]interface{}) {
}

func (t *cmqWriterJson) input_extent(row map[string]interface{}) {
}

func (t *cmqWriterJson) store_extent(row map[string]interface{}) {
}

func (t *cmqWriterJson) close() {
}

type cmqWriterNull struct{}

func newCmqWriterNull(ctx *outContext) *cmqWriterNull {
	return &cmqWriterNull{}
}

func (t *cmqWriterNull) destination(row map[string]interface{}) {
}

func (t *cmqWriterNull) consumer_group(row map[string]interface{}) {
}

func (t *cmqWriterNull) extent(row map[string]interface{}) {
}

func (t *cmqWriterNull) consumer_group_extent(row map[string]interface{}) {
}

func (t *cmqWriterNull) input_extent(row map[string]interface{}) {
}

func (t *cmqWriterNull) store_extent(row map[string]interface{}) {
}

func (t *cmqWriterNull) close() {
}
