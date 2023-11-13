package main

import (
	"bytes"
	"context"
	"embed"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"syscall"
	"time"

	"github.com/geofduf/run-length/sequence"
)

const (
	sequenceFrequency = 15
	maxNumberOfPoints = 380
	serializeFlag     = sequence.SerializeCount | sequence.SerializeMean
	maskTime          = "2006-01-02 15:04:05"

	statusOK      = "ok"
	statusWarning = "warning"
	statusError   = "error"
)

var (
	aggregations   = []int64{15, 30, 60, 120, 300, 600, 900, 1200, 1800, 3600, 7200, 14400, 43200, 86400}
	validStatement = regexp.MustCompile(`^\w+ [012](?: \d+)?$`)
)

//go:embed assets
var assets embed.FS

type server struct {
	store *sequence.Store
}

func main() {
	var listen, dumpFile string
	var dumpInterval, retentionPolicy int
	flag.StringVar(&listen, "l", "127.0.0.1:8080", "Listening address:port")
	flag.StringVar(&dumpFile, "f", "./store.dump", "Full path to dump file")
	flag.IntVar(&dumpInterval, "i", 0, "Dump interval in seconds (0 or less to disable)")
	flag.IntVar(&retentionPolicy, "r", 365, "Retention policy in days (0 or less to disable)")
	flag.Parse()

	html, err := assets.ReadFile("assets/templates/index.html")
	if err != nil {
		log.Fatal(err)
	}

	static, err := fs.Sub(assets, "assets/static")
	if err != nil {
		log.Fatal(err)
	}

	s := &server{store: sequence.NewStore()}

	if _, err := os.Stat(dumpFile); errors.Is(err, os.ErrNotExist) {
		log.Println("file does not exist, starting with empty store")
	} else {
		f, err := os.ReadFile(dumpFile)
		if err != nil {
			log.Fatalf("error reading file: %s", err)
		}
		if err := s.store.Load(f); err != nil {
			log.Fatalf("error loading store: %s", err)
		}
	}

	if dumpInterval > 0 {
		go func() {
			for range time.Tick(time.Duration(dumpInterval) * time.Second) {
				s.dump(dumpFile)
			}
		}()
	}

	if retentionPolicy > 0 {
		retention := time.Duration(retentionPolicy) * 86400 * time.Second
		go func() {
			for range time.Tick(86400 * time.Second) {
				s.store.TrimLeft(time.Now().Add(-retention).Truncate(time.Duration(sequenceFrequency) * time.Second))
			}
		}()
	}

	httpServer := http.Server{Addr: listen}

	closed := make(chan struct{})
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
		<-sig
		log.Println("graceful shutdown")
		s.dump(dumpFile)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		httpServer.Shutdown(ctx)
		close(closed)
	}()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Write(html)
	})

	http.HandleFunc("/insert/", s.handlerInsert)
	http.HandleFunc("/query/", s.handlerQuery)
	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.FS(static))))

	log.Printf("listening on %s", listen)

	if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}
	<-closed
}

func (s *server) dump(f string) {
	buf, err := s.store.Dump()
	if err != nil {
		log.Printf("error dumping store: %s", err)
		return
	}
	err = os.WriteFile(f, buf, 0660)
	if err != nil {
		log.Printf("error writing file: %s", err)
		return
	}
	log.Printf("writing store to file (%d bytes)", len(buf))
}

func (s *server) handlerInsert(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeResponse(w, http.StatusMethodNotAllowed, statusError, "method not allowed", nil)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeResponse(w, http.StatusBadRequest, statusError, "error reading request body", nil)
		log.Printf("error reading request body: %s", err)
		return
	}

	defaultValueTimestamp := time.Now()
	defaultSequenceTimestamp := defaultValueTimestamp.Truncate(time.Duration(sequenceFrequency) * time.Second)

	lines := bytes.Split(body, []byte("\n"))

	mapping := make([]int, len(lines))

	var n int
	for i := 0; i < len(lines); i++ {
		if !validStatement.Match(lines[i]) {
			log.Printf("error parsing statement %d", i+1)
			continue
		}
		mapping[n] = i
		n++
	}

	statements := make([]sequence.Statement, n)

	for i := 0; i < n; i++ {
		line := lines[mapping[i]]
		p := bytes.IndexByte(line, ' ')
		// verbose conversion for the sake of clarity
		var value uint8
		switch line[p+1] {
		case '0':
			value = sequence.StateInactive
		case '1':
			value = sequence.StateActive
		case '2':
			value = sequence.StateUnknown
		default:
			log.Panic("poor validation panic")
		}
		valueTimestamp, sequenceTimestamp := defaultValueTimestamp, defaultSequenceTimestamp
		if len(line) > p+2 {
			x, err := strconv.Atoi(string(line[p+3:]))
			if err != nil {
				log.Panic("poor validation panic")
			}
			valueTimestamp = time.Unix(int64(x), 0)
			sequenceTimestamp = valueTimestamp.Truncate(time.Duration(sequenceFrequency) * time.Second)
		}
		statements[i] = sequence.Statement{
			Key:                 string(line[:p]),
			Timestamp:           valueTimestamp,
			Value:               value,
			Type:                sequence.StatementAdd,
			CreateIfNotExists:   true,
			CreateWithTimestamp: sequenceTimestamp,
			CreateWithFrequency: sequenceFrequency,
		}
	}

	result := s.store.Batch(statements)
	if result.HasErrors() {
		for i, err := range result.ErrorVars() {
			if err != nil {
				log.Printf("error executing statement %d: %s", mapping[i]+1, err)
				n--
			}
		}
	}

	status := statusOK
	if n != len(lines) {
		status = statusWarning
	}

	writeResponse(w, http.StatusOK, status, fmt.Sprintf("processed %d/%d statement(s)", n, len(lines)), nil)
}

func (s *server) handlerQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeResponse(w, http.StatusBadRequest, statusError, "method is not allowed", nil)
		return
	}

	key := r.FormValue("key")

	args, err := newQueryArgs(r.FormValue("start"), r.FormValue("end"))
	if err != nil {
		writeResponse(w, http.StatusBadRequest, statusError, err.Error(), nil)
		return
	}

	// until better error handling
	if _, ok := s.store.Get(key); !ok {
		writeResponse(w, http.StatusBadRequest, statusError, "key does not exist", nil)
		return
	}

	qs, err := s.store.Query(key, args.start, args.end, args.interval)
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, statusError, "an unexpected error occurred", nil)
		log.Printf("error executing query: %s", err)
		return
	}

	message := fmt.Sprintf("%d row(s) returned (interval %ds)", len(qs.Count), int(args.interval.Seconds()))
	writeResponse(w, http.StatusOK, statusOK, message, qs.Serialize("", time.UTC, 2, serializeFlag))
}

type queryArgs struct {
	start    time.Time
	end      time.Time
	interval time.Duration
}

func newQueryArgs(start, end string) (queryArgs, error) {
	v, err := strconv.Atoi(start)
	if err != nil {
		return queryArgs{}, errors.New("error parsing start date")
	}
	x := time.Unix(ceilInt64(int64(v), sequenceFrequency), 0)

	v, err = strconv.Atoi(end)
	if err != nil {
		return queryArgs{}, errors.New("error parsing end date")
	}
	y := time.Unix(int64(v), 0)

	if x.After(y) {
		return queryArgs{}, errors.New("range is not valid")
	}

	scope := y.Unix() - x.Unix()

	var aggregation int64
	for _, v := range aggregations {
		if scope/v <= maxNumberOfPoints {
			aggregation = v
			break
		}
	}

	if aggregation == 0 {
		return queryArgs{}, errors.New("range is too large")
	}

	return queryArgs{start: x, end: y, interval: time.Duration(aggregation) * time.Second}, nil
}

func ceilInt64(x int64, step int64) int64 {
	r := x % step
	if r != 0 {
		return x + step - r
	}
	return x
}

func writeResponse(w http.ResponseWriter, code int, status, message string, data []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if data == nil {
		fmt.Fprintf(w, `{"code":%d,"status":"%s","message":"%s"}`, code, status, message)
		return
	}
	prefix := fmt.Sprintf(`{"code":%d,"status":"%s","message":"%s","data":`, code, status, message)
	var buf bytes.Buffer
	buf.WriteString(prefix)
	buf.Write(data)
	buf.WriteByte('}')
	w.Write(buf.Bytes())
}
