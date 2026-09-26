package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/datashaman/provision-example-async/internal/example"
	"github.com/datashaman/provision-example-async/internal/rabbit"
	"github.com/datashaman/provision-example-async/internal/worker"
)

const version = "0.2.0"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "provision-example-async-worker: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("provision-example-async-worker", flag.ContinueOnError)
	flags.SetOutput(stderr)
	brokerURLFile := flags.String("broker-url-file", "", "path to a file containing the AMQP URL")
	queue := flags.String("queue", "", "existing RabbitMQ quorum queue")
	applicationRevision := flags.String("application-revision", "", "immutable Provision Application Revision identity")
	artifactDigest := flags.String("artifact-digest", "", "immutable Worker artifact SHA-256 digest")
	gateFile := flags.String("gate-file", "", "intake gate file; exact content 'open' enables consumption")
	stateFile := flags.String("state-file", "", "atomic JSON worker-state file")
	evidenceFile := flags.String("evidence-file", "", "append-only JSON Lines processing evidence")
	ledgerDir := flags.String("ledger-dir", "", "durable idempotency ledger directory")
	holdDir := flags.String("hold-dir", "", "directory containing explicit hold-release controls")
	pollInterval := flags.Duration("poll-interval", 100*time.Millisecond, "gate and hold polling interval")
	showVersion := flags.Bool("version", false, "print the Worker version and exit")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	if *showVersion {
		_, err := fmt.Fprintln(stdout, version)
		return err
	}
	config := worker.RuntimeConfig{
		ApplicationRevision:  example.ApplicationRevision(*applicationRevision),
		WorkerArtifactDigest: example.ArtifactDigest(*artifactDigest),
		Queue:                *queue,
		GateFile:             *gateFile,
		StateFile:            *stateFile,
		PollInterval:         *pollInterval,
	}
	if err := config.Validate(); err != nil {
		return err
	}
	if *evidenceFile == "" || *ledgerDir == "" || *holdDir == "" {
		return fmt.Errorf("evidence file, ledger directory, and hold directory are required")
	}
	recorder, err := worker.NewRecorder(*evidenceFile)
	if err != nil {
		return fmt.Errorf("open evidence recorder: %w", err)
	}
	defer recorder.Close()
	processor, err := worker.NewProcessor(*ledgerDir, *holdDir, recorder, config.ApplicationRevision, config.WorkerArtifactDigest)
	if err != nil {
		return err
	}
	consumer, err := rabbit.OpenConsumer(*brokerURLFile, *queue)
	if err != nil {
		return err
	}
	defer consumer.Close()
	return worker.Run(ctx, config, consumer, processor, recorder)
}
