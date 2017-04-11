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

package common

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"runtime/debug"

	"gopkg.in/yaml.v2"

	"github.com/uber/cherami-server/common/configure"
)

// HTTPHandler contains the http handlers
// for controller
type HTTPHandler struct {
	cfg     configure.CommonAppConfig
	service SCommon
}

const (
	serviceURLParam = "service"
)

var serviceNames = []string{InputServiceName, OutputServiceName, StoreServiceName, ControllerServiceName, FrontendServiceName, ReplicatorServiceName}

// NewHTTPHandler returns a new instance of
// http handler. This call must be followed
// by a call to httpHandler.Register().
func NewHTTPHandler(cfg configure.CommonAppConfig, service SCommon) *HTTPHandler {
	return &HTTPHandler{
		cfg:     cfg,
		service: service,
	}
}

// Register registers all the http handlers
// supported by controller. This method does
// not start a http server.
func (handler *HTTPHandler) Register(mux *http.ServeMux) {
	// handler that returns all healthy hosts for a service by querying ringpop
	mux.Handle("/help", http.HandlerFunc(handler.help))
	mux.Handle("/configz", http.HandlerFunc(handler.configz))
	mux.Handle("/heapdumpz", http.HandlerFunc(handler.heapdumpz))
	mux.Handle("/gethosts", http.HandlerFunc(handler.getHosts))
	mux.Handle("/memstatsdumpz", http.HandlerFunc(handler.memstatsdumpz))

	// we need to register the pprof endpoints as well since we are not
	// using the default mux now!
	mux.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	mux.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
	mux.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	mux.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
	mux.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))

	// Register service specific upgrade handlers
	mux.Handle("/upgrade", http.HandlerFunc(handler.service.UpgradeHandler))
}

// Help is the http handler for /help
func (handler *HTTPHandler) help(w http.ResponseWriter, r *http.Request) {

	helpMsg := `Supported APIs

                   /configz
                      Dumps the config params

		   /heapdumpz
		      Dumps the heap stats

		   /memstatsdumpz
                      Dumps the runtime memstats info

                   /gethosts?service=serviceName
                      Returns all the healthy hosts for a service by
                      querying Ringpop. Allowed values for serviceName are
                      [ inputhost, outputhost, storehost, controller, frontendhost ]
                      Defaults to all services, if not specified.`

	fmt.Fprintln(w, helpMsg)
}

// Configz is the http handler for /configz. It dumps the config
// for this service
func (handler *HTTPHandler) configz(w http.ResponseWriter, r *http.Request) {
	if buf, err := yaml.Marshal(handler.cfg); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("%v", err)))
	} else {
		w.Write(buf)
	}
}

// GetHosts is a http handler that returns all healthy hosts
// for a given service by querying ringpop.
func (handler *HTTPHandler) getHosts(w http.ResponseWriter, r *http.Request) {

	rpm := handler.service.GetRingpopMonitor()

	servicesToQuery := serviceNames

	serviceParam := r.FormValue(serviceURLParam)
	if len(serviceParam) > 0 {
		servicesToQuery = []string{serviceParam}
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	switch serviceParam {
	case ControllerServiceName:
		host, err := rpm.FindHostForKey(serviceParam, serviceParam)
		if err != nil {
			fmt.Fprintf(w, "error=%v\n", err)
			return
		}
		fmt.Fprintf(w, "primary=%v\n", host)
		fallthrough
	default:
		for _, s := range servicesToQuery {
			hosts, err := rpm.GetHosts(s)
			if err != nil {
				fmt.Fprintf(w, "service=%v, error=%v\n", s, err)
				continue
			}
			for _, h := range hosts {
				fmt.Fprintf(w, "service=%v, uuid=%v, addr=%v\n", s, h.UUID, h.Addr)
			}
		}
	}
}

// Heapdumpz is the http handler for /heapdumpz. It dumps the run time
// heap stats for this process.
func (handler *HTTPHandler) heapdumpz(w http.ResponseWriter, r *http.Request) {
	var name = "heapdump.out"
	var path = fmt.Sprintf("%s/%s", os.TempDir(), name)
	switch r.Method {
	case "POST":
		if f, err := os.Create(path); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprintf("%v", err)))
		} else {
			defer f.Close()
			debug.WriteHeapDump(f.Fd())
			// Redirect caller to download the heapdump output via HTTP GET.
			http.Redirect(w, r, "/heapdumpz", http.StatusSeeOther)
		}
	case "GET":
		if f, err := os.Open(path); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprintf("%v", err)))
		} else if fi, e := os.Stat(path); e != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprintf("%v", e)))
		} else {
			defer f.Close()
			w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", name))
			http.ServeContent(w, r, name, fi.ModTime(), f)
		}
	}
}

// memstatsdumpz is the http handler for /memstatsdumpz. It dumps the run time
// memory stats for this process.
func (handler *HTTPHandler) memstatsdumpz(w http.ResponseWriter, r *http.Request) {
	memStats := &runtime.MemStats{}
	runtime.ReadMemStats(memStats)

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	fmt.Fprintf(w, "Alloc (Bytes allocated and not yet freed)=%v\n", memStats.Alloc)
	fmt.Fprintf(w, "Mallocs (number of mallocs)=%v\n", memStats.Mallocs)
	fmt.Fprintf(w, "Frees (number of frees)=%v\n", memStats.Frees)
	fmt.Fprintf(w, "Lookups (number of pointer lookups)=%v\n", memStats.Lookups)

	fmt.Fprintf(w, "StackInuse (bytes used by stack allocator)=%v\n", memStats.StackInuse)
	fmt.Fprintf(w, "NextGC (next collection will happen when heapAlloc >= this)=%v\n", memStats.NextGC)

	// GC stats in a readable format
	gcStats := &debug.GCStats{}
	debug.ReadGCStats(gcStats)

	fmt.Fprintf(w, "LastGC (time of last collection)=%v\n", gcStats.LastGC)
	fmt.Fprintf(w, "NumGC (number of collections)=%v\n", gcStats.NumGC)
	fmt.Fprintf(w, "PauseTotal (total pause for all collections)=%v\n", gcStats.PauseTotal)
	fmt.Fprintf(w, "PauseHistory (most recent first)\n\t")
	// just dump the most recent 5
	for _, pHistory := range gcStats.Pause[:5] {
		fmt.Fprintf(w, "\t%v;", pHistory)
	}
	fmt.Fprintf(w, "\n")
	fmt.Fprintf(w, "PauseEnd (pause end times history)\n\t")
	for _, pEnd := range gcStats.PauseEnd[:5] {
		fmt.Fprintf(w, "\t%v;", pEnd)
	}
	fmt.Fprintf(w, "\n")
}
