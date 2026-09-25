package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/datashaman/provision-example-async/internal/example"
	"github.com/datashaman/provision-example-async/internal/rabbit"
	"github.com/datashaman/provision-example-async/internal/task"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "provision-example-async-task: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("provision-example-async-task", flag.ContinueOnError)
	flags.SetOutput(stderr)
	brokerURLFile := flags.String("broker-url-file", "", "path to a file containing the AMQP URL")
	queue := flags.String("queue", "", "existing RabbitMQ quorum queue")
	revision := flags.String("revision", "", "immutable Task revision identity")
	invocation := flags.String("invocation", "", "stable Task Invocation identity")
	count := flags.Int("count", 1, "number of deterministic messages to publish")
	behavior := flags.String("behavior", string(example.BehaviorProcess), "message behavior: process, hold, fail-once, or reject")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	config := task.Config{Queue: *queue, Revision: *revision, InvocationID: *invocation, Count: *count, Behavior: example.Behavior(*behavior)}
	if err := config.Validate(); err != nil {
		return err
	}
	publisher, err := rabbit.OpenPublisher(*brokerURLFile)
	if err != nil {
		return err
	}
	defer publisher.Close()
	return task.Run(ctx, config, publisher, stdout)
}
