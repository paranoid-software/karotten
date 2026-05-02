// karotten — Docker → RabbitMQ log sink.
//
// Listens to every running container's stdout/stderr via the Docker
// Engine HTTP API and publishes each line to RabbitMQ on a topic
// exchange. Routing key shape: <project>.<service>.<stream>.
//
// Talks to the Docker daemon over its unix socket using stdlib net/http;
// no docker SDK dependency. Only attaches to containers in `running`
// state — exited ones are ignored entirely.
//
// Resilience layers (failure modes seen during development drove these):
//
//  1. ResponseHeaderTimeout on the docker HTTP client so a frozen daemon
//     can't make GETs hang indefinitely — initial header round-trips
//     time out after 15s.
//  2. Events stream watchdog: if no docker event arrives for
//     `eventsIdleTimeout`, reconnect the events stream. Catches the case
//     where the kernel doesn't propagate FIN promptly after a daemon
//     restart and our read sits forever.
//  3. Daemon /_ping health check every 30s. After 3 consecutive
//     failures, the process exits non-zero so a supervisor (systemd,
//     etc.) can restart it from a clean state. Last-resort backstop.
//  4. AMQP publisher reconnects automatically with backoff on any error.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	dockerAPIVersion       = "v1.43"
	publishBuffer          = 8192
	timestampFmt           = time.RFC3339Nano
	maxLineBytes           = 1 << 20 // 1 MiB cap per line
	headerTimeout          = 15 * time.Second
	idleConnTimeout        = 60 * time.Second
	eventsIdleTimeout      = 3 * time.Minute
	healthCheckInterval    = 30 * time.Second
	healthCheckMaxFailures = 3
	amqpReconnectBackoff   = 2 * time.Second
	eventsReconnectBackoff = 2 * time.Second
)

type message struct {
	routingKey string
	body       []byte
}

type envelope struct {
	Time          string `json:"timestamp"`
	Project       string `json:"project"`
	Service       string `json:"service"`
	ContainerName string `json:"container_name"`
	ContainerID   string `json:"container_id"`
	Image         string `json:"image"`
	Stream        string `json:"stream"`
	Message       string `json:"message"`
}

type containerSummary struct {
	ID    string   `json:"Id"`
	Names []string `json:"Names"`
	Image string   `json:"Image"`
	State string   `json:"State"`
}

type containerInspect struct {
	ID    string `json:"Id"`
	Name  string `json:"Name"`
	State struct {
		Running bool `json:"Running"`
	} `json:"State"`
	Config struct {
		Image  string            `json:"Image"`
		Tty    bool              `json:"Tty"`
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
}

type dockerEvent struct {
	Action string `json:"Action"`
	Type   string `json:"Type"`
	Actor  struct {
		ID string `json:"ID"`
	} `json:"Actor"`
	TimeNano int64 `json:"timeNano"`
}

type shipper struct {
	dockerClient *http.Client
	dockerHost   string
	exchange     string
	publish      chan message

	activeMu sync.Mutex
	active   map[string]context.CancelFunc

	dropped atomic.Uint64
}

func main() {
	rmqURL := mustEnv("RABBITMQ_URL")
	exchange := envDefault("EXCHANGE", "containers-logs")
	dockerSock := envDefault("DOCKER_SOCK", "/var/run/docker.sock")

	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	s := &shipper{
		dockerClient: newDockerClient(dockerSock),
		dockerHost:   "http://docker/" + dockerAPIVersion,
		exchange:     exchange,
		publish:      make(chan message, publishBuffer),
		active:       make(map[string]context.CancelFunc),
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() { defer wg.Done(); s.runPublisher(rootCtx, rmqURL) }()

	wg.Add(1)
	go func() { defer wg.Done(); s.runDaemonHealthCheck(rootCtx) }()

	wg.Add(1)
	go func() { defer wg.Done(); s.dropReporter(rootCtx) }()

	wg.Add(1)
	go func() { defer wg.Done(); s.watchEvents(rootCtx) }()

	containers, err := s.listRunning(rootCtx)
	if err != nil {
		log.Fatalf("docker list: %v", err)
	}
	log.Printf("found %d running containers at startup", len(containers))
	for _, c := range containers {
		s.startStream(rootCtx, c.ID)
	}

	<-rootCtx.Done()
	log.Println("shutdown signal received; cancelling streams")
	s.cancelAll()
	wg.Wait()
	log.Println("shutdown complete")
}

// --- Docker HTTP client over unix socket -----------------------------------

func newDockerClient(sock string) *http.Client {
	dialer := &net.Dialer{}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, "unix", sock)
		},
		// Cap the time spent waiting for the daemon to send response
		// headers. Body reads (event stream, log streams) are not bound
		// by this — they're meant to last forever.
		ResponseHeaderTimeout: headerTimeout,
		// Drop pooled idle connections promptly so a daemon restart's
		// stale FDs don't sit around in the pool.
		IdleConnTimeout: idleConnTimeout,
		// Don't keep more idle conns per host than we need; we have
		// at most ~25 concurrent in-use streams.
		MaxIdleConnsPerHost: 4,
	}
	return &http.Client{
		Transport: transport,
		Timeout:   0, // 0 = no whole-request timeout; streams must be unbounded
	}
}

func (s *shipper) get(ctx context.Context, path string, q url.Values) (*http.Response, error) {
	full := s.dockerHost + path
	if len(q) > 0 {
		full += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, full, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.dockerClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		resp.Body.Close()
		return nil, fmt.Errorf("docker %s: %d %s", path, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return resp, nil
}

func (s *shipper) listRunning(ctx context.Context) ([]containerSummary, error) {
	resp, err := s.get(ctx, "/containers/json", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out []containerSummary
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *shipper) inspect(ctx context.Context, id string) (*containerInspect, error) {
	resp, err := s.get(ctx, "/containers/"+id+"/json", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out containerInspect
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

// --- Daemon health check (the "everything else failed" backstop) ----------

func (s *shipper) runDaemonHealthCheck(ctx context.Context) {
	t := time.NewTicker(healthCheckInterval)
	defer t.Stop()
	failures := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			resp, err := s.get(pingCtx, "/_ping", nil)
			cancel()
			if err != nil {
				failures++
				log.Printf("docker daemon ping failed (%d/%d): %v", failures, healthCheckMaxFailures, err)
				if failures >= healthCheckMaxFailures {
					log.Fatalf("docker daemon unreachable for %d consecutive checks; exiting so supervisor can restart us", failures)
				}
				// Drop any pooled stale conns so the next request retries fresh.
				if t, ok := s.dockerClient.Transport.(*http.Transport); ok {
					t.CloseIdleConnections()
				}
				continue
			}
			resp.Body.Close()
			if failures > 0 {
				log.Printf("docker daemon reachable again")
			}
			failures = 0
		}
	}
}

// --- Event stream ----------------------------------------------------------

func (s *shipper) watchEvents(ctx context.Context) {
	for ctx.Err() == nil {
		err := s.eventStreamOnce(ctx)
		if err != nil && ctx.Err() == nil {
			log.Printf("docker events: %v; reconnecting in %v", err, eventsReconnectBackoff)
			select {
			case <-time.After(eventsReconnectBackoff):
			case <-ctx.Done():
				return
			}
		}
	}
}

func (s *shipper) eventStreamOnce(ctx context.Context) error {
	q := url.Values{}
	// Filter to container-related events only. We get every action
	// (start, die, exec_*, attach, etc.) which serves as a steady
	// keep-alive — most stacks emit several per minute even at idle.
	q.Set("filters", `{"type":["container"]}`)

	resp, err := s.get(ctx, "/events", q)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	log.Println("subscribed to docker events")

	// Decode in a goroutine so we can apply an idle watchdog on top.
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type frame struct {
		ev  dockerEvent
		err error
	}
	frames := make(chan frame)
	go func() {
		dec := json.NewDecoder(resp.Body)
		for {
			var ev dockerEvent
			err := dec.Decode(&ev)
			select {
			case frames <- frame{ev: ev, err: err}:
			case <-streamCtx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	idle := time.NewTimer(eventsIdleTimeout)
	defer idle.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case f := <-frames:
			if f.err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return f.err
			}
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}
			idle.Reset(eventsIdleTimeout)
			switch f.ev.Action {
			case "start":
				s.startStream(ctx, f.ev.Actor.ID)
			case "die":
				s.stopStream(f.ev.Actor.ID)
			}
		case <-idle.C:
			return fmt.Errorf("no docker events received in %v; forcing reconnect", eventsIdleTimeout)
		}
	}
}

// --- Per-container streaming ----------------------------------------------

func (s *shipper) startStream(parent context.Context, id string) {
	s.activeMu.Lock()
	if _, ok := s.active[id]; ok {
		s.activeMu.Unlock()
		return
	}
	streamCtx, cancel := context.WithCancel(parent)
	s.active[id] = cancel
	s.activeMu.Unlock()

	go func() {
		defer func() {
			s.activeMu.Lock()
			delete(s.active, id)
			s.activeMu.Unlock()
			cancel()
		}()
		s.streamContainer(streamCtx, id)
	}()
}

func (s *shipper) stopStream(id string) {
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	if cancel, ok := s.active[id]; ok {
		cancel()
	}
}

func (s *shipper) cancelAll() {
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	for _, cancel := range s.active {
		cancel()
	}
}

func (s *shipper) streamContainer(ctx context.Context, id string) {
	info, err := s.inspect(ctx, id)
	if err != nil {
		log.Printf("inspect %s: %v", short(id), err)
		return
	}
	if !info.State.Running {
		// Skip everything that isn't actively running — the whole point
		// of this design is to never open a log stream for an exited
		// container.
		return
	}

	name := strings.TrimPrefix(info.Name, "/")
	project := info.Config.Labels["com.docker.compose.project"]
	if project == "" {
		project = "standalone"
	}
	service := info.Config.Labels["com.docker.compose.service"]
	if service == "" {
		// Container not started by compose. Fall back to the container
		// name so the routing key still has a meaningful second segment.
		service = name
	}
	nameSeg := strings.ReplaceAll(name, ".", "-")
	projectSeg := strings.ReplaceAll(project, ".", "-")
	serviceSeg := strings.ReplaceAll(service, ".", "-")

	q := url.Values{}
	q.Set("stdout", "1")
	q.Set("stderr", "1")
	q.Set("follow", "1")
	q.Set("tail", "0")
	q.Set("since", fmt.Sprintf("%d", time.Now().Unix()))

	resp, err := s.get(ctx, "/containers/"+id+"/logs", q)
	if err != nil {
		log.Printf("logs %s: %v", short(id), err)
		return
	}
	defer resp.Body.Close()

	log.Printf("attached %s/%s (%s)", projectSeg, serviceSeg, short(id))
	defer log.Printf("detached %s/%s", projectSeg, serviceSeg)

	common := envelope{
		Project:       projectSeg,
		Service:       serviceSeg,
		ContainerName: nameSeg,
		ContainerID:   short(id),
		Image:         info.Config.Image,
	}

	if info.Config.Tty {
		s.consumeTTY(ctx, resp.Body, common)
		return
	}
	s.consumeMultiplexed(ctx, resp.Body, common)
}

// consumeTTY: when tty=true, the log stream is raw bytes (no framing).
// Treat everything as stdout.
func (s *shipper) consumeTTY(ctx context.Context, r io.Reader, common envelope) {
	common.Stream = "stdout"
	rk := common.Project + "." + common.Service + ".stdout"
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 4096), maxLineBytes)
	for sc.Scan() && ctx.Err() == nil {
		line := bytes.TrimRight(sc.Bytes(), "\r")
		if len(line) == 0 {
			continue
		}
		s.send(rk, buildBody(common, line))
	}
}

// consumeMultiplexed reads Docker's per-frame format used when tty=false.
// Each frame is 8 bytes of header + payload:
//
//	[0]    stream type: 1=stdout, 2=stderr (0=stdin is unused for log reads)
//	[1..3] reserved (zero)
//	[4..7] big-endian uint32 payload length
func (s *shipper) consumeMultiplexed(ctx context.Context, r io.Reader, common envelope) {
	stdoutBuf := newLineBuffer(s, common, "stdout")
	stderrBuf := newLineBuffer(s, common, "stderr")
	defer stdoutBuf.flush()
	defer stderrBuf.flush()

	header := make([]byte, 8)
	for ctx.Err() == nil {
		if _, err := io.ReadFull(r, header); err != nil {
			return
		}
		size := binary.BigEndian.Uint32(header[4:8])
		if size == 0 {
			continue
		}
		if size > maxLineBytes {
			if _, err := io.CopyN(io.Discard, r, int64(size)); err != nil {
				return
			}
			continue
		}
		payload := make([]byte, size)
		if _, err := io.ReadFull(r, payload); err != nil {
			return
		}
		switch header[0] {
		case 1:
			stdoutBuf.write(payload)
		case 2:
			stderrBuf.write(payload)
		}
	}
}

// lineBuffer accumulates frame payloads until it has full lines, then publishes.
type lineBuffer struct {
	shipper *shipper
	common  envelope
	rk      string
	buf     []byte
}

func newLineBuffer(s *shipper, common envelope, stream string) *lineBuffer {
	common.Stream = stream
	return &lineBuffer{
		shipper: s,
		common:  common,
		rk:      common.Project + "." + common.Service + "." + stream,
	}
}

func (lb *lineBuffer) write(p []byte) {
	lb.buf = append(lb.buf, p...)
	for {
		idx := bytes.IndexByte(lb.buf, '\n')
		if idx < 0 {
			break
		}
		line := bytes.TrimRight(lb.buf[:idx], "\r")
		lb.buf = lb.buf[idx+1:]
		if len(line) > 0 {
			lb.shipper.send(lb.rk, buildBody(lb.common, line))
		}
	}
	if len(lb.buf) > maxLineBytes {
		lb.shipper.send(lb.rk, buildBody(lb.common, lb.buf))
		lb.buf = nil
	}
}

func (lb *lineBuffer) flush() {
	if len(lb.buf) > 0 {
		lb.shipper.send(lb.rk, buildBody(lb.common, lb.buf))
		lb.buf = nil
	}
}

func buildBody(common envelope, line []byte) []byte {
	common.Time = time.Now().UTC().Format(timestampFmt)
	common.Message = string(line)
	b, _ := json.Marshal(common)
	return b
}

// send queues a message for the AMQP publisher; drops on overflow.
func (s *shipper) send(rk string, body []byte) {
	select {
	case s.publish <- message{routingKey: rk, body: body}:
		if os.Getenv("DEBUG") != "" {
			log.Printf("DEBUG send rk=%s len=%d", rk, len(body))
		}
	default:
		s.dropped.Add(1)
	}
}

// --- AMQP publisher -------------------------------------------------------

func (s *shipper) runPublisher(ctx context.Context, url string) {
	for ctx.Err() == nil {
		conn, ch, err := dialAMQP(url, s.exchange)
		if err != nil {
			log.Printf("amqp dial: %v; retrying in %v", err, amqpReconnectBackoff)
			select {
			case <-time.After(amqpReconnectBackoff):
			case <-ctx.Done():
				return
			}
			continue
		}
		log.Printf("amqp connected; publishing to exchange %q", s.exchange)
		s.publishLoop(ctx, ch)
		_ = ch.Close()
		_ = conn.Close()
	}
}

func dialAMQP(url, exchange string) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.DialConfig(url, amqp.Config{
		Heartbeat: 30 * time.Second,
		Locale:    "en_US",
	})
	if err != nil {
		return nil, nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	if err := ch.ExchangeDeclarePassive(exchange, "topic", true, false, false, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, nil, fmt.Errorf("exchange %q passive declare: %w", exchange, err)
	}
	return conn, ch, nil
}

func (s *shipper) publishLoop(ctx context.Context, ch *amqp.Channel) {
	closeNotify := ch.NotifyClose(make(chan *amqp.Error, 1))
	for {
		select {
		case <-ctx.Done():
			return
		case err := <-closeNotify:
			log.Printf("amqp channel closed: %v", err)
			return
		case m := <-s.publish:
			err := ch.PublishWithContext(ctx, s.exchange, m.routingKey, false, false, amqp.Publishing{
				ContentType:  "application/json",
				Body:         m.body,
				DeliveryMode: amqp.Transient,
				Timestamp:    time.Now().UTC(),
			})
			if err != nil {
				log.Printf("amqp publish: %v", err)
				return
			}
		}
	}
}

func (s *shipper) dropReporter(ctx context.Context) {
	t := time.NewTicker(60 * time.Second)
	defer t.Stop()
	var last uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			cur := s.dropped.Load()
			if cur != last {
				log.Printf("dropped (broker behind / disconnected): +%d in last 60s, total %d", cur-last, cur)
				last = cur
			}
		}
	}
}

// --- helpers --------------------------------------------------------------

func short(id string) string {
	if len(id) >= 12 {
		return id[:12]
	}
	return id
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("env %s required", k)
	}
	return v
}

func envDefault(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
