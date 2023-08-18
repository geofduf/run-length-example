package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
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
	validStatement = regexp.MustCompile(`^(\w+) ([012])(?: (\d+))?$`)
)

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

	http.HandleFunc("/insert/", s.handlerInsert)
	http.HandleFunc("/query/", s.handlerQuery)

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

	var statements []sequence.Statement
	var indexMapping []int

	defaultValueTimestamp := time.Now()
	defaultSequenceTimestamp := defaultValueTimestamp.Truncate(time.Duration(sequenceFrequency) * time.Second)
	skipped := 0

	lines := bytes.Split(body, []byte("\n"))

	for i, line := range lines {
		match := validStatement.FindSubmatch(line)
		if match == nil {
			log.Printf("error parsing statement %d", i+1)
			skipped++
			continue
		}
		// verbose conversion for the sake of clarity
		var value uint8
		switch match[2][0] {
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
		if len(match[3]) > 0 {
			x, err := strconv.Atoi(string(match[3]))
			if err != nil {
				log.Panic("poor validation panic")
			}
			valueTimestamp = time.Unix(int64(x), 0)
			sequenceTimestamp = valueTimestamp.Truncate(time.Duration(sequenceFrequency) * time.Second)
		}
		statements = append(statements, sequence.Statement{
			Key:                 string(match[1]),
			Timestamp:           valueTimestamp,
			Value:               value,
			Type:                sequence.StatementAdd,
			CreateIfNotExists:   true,
			CreateWithTimestamp: sequenceTimestamp,
			CreateWithFrequency: sequenceFrequency,
		})
		indexMapping = append(indexMapping, i)
	}

	result := s.store.Batch(statements)
	if result.HasErrors() {
		for i, err := range result.ErrorVars() {
			if err != nil {
				log.Printf("error executing statement %d: %s", indexMapping[i]+1, err)
				skipped++
			}
		}
	}

	status := statusOK
	if skipped > 0 {
		status = statusWarning
	}

	n := len(lines)
	writeResponse(w, http.StatusOK, status, fmt.Sprintf("processed %d/%d statement(s)", n-skipped, n), nil)
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

	qs, err := s.store.Query(key, args.start, args.end, args.interval)
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, statusError, "error executing query", nil)
		log.Printf("error executing query: %s", err)
		return
	}

	message := fmt.Sprintf("%d row(s) returned (interval %ds)", len(qs.Count), int(args.interval.Seconds()))
	writeResponse(w, http.StatusOK, statusOK, message, qs.Serialize(maskTime, time.UTC, 2, serializeFlag))
}

type queryArgs struct {
	start    time.Time
	end      time.Time
	interval time.Duration
}

func newQueryArgs(start, end string) (queryArgs, error) {
	x, err := time.Parse(maskTime, start)
	if err != nil {
		return queryArgs{}, errors.New("error parsing start date")
	}

	y, err := time.Parse(maskTime, end)
	if err != nil {
		return queryArgs{}, errors.New("error parsing end date")
	}

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
