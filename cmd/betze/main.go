package main

import (
	"fmt"
	"log"
	"os"

	"github.com/urfave/cli/v2"
)

type missingArgError struct {
	arg string
}

func (e *missingArgError) Error() string {
	return fmt.Sprintf("missing argument: <%s> is required", e.arg)
}

type unknownArgValueError struct {
	arg      string
	val      string
	expected *string
}

func (e *unknownArgValueError) Error() string {
	msg := fmt.Sprintf("unknown value for argument <%s>: \"%s\". ", e.arg, e.val)
	if e.expected != nil {
		msg += fmt.Sprintf("expected %s", *e.expected)
	}
	return msg
}

const (
	joda_host_opt     = "joda-host"
	dataset_providers = "{JODA}"
)

func initialize_cli(c *cli.Context) error {
	if logfile := c.String("logfile"); len(logfile) > 0 {
		file, err := os.OpenFile(logfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			return err
		}
		log.SetOutput(file)
	}
	return nil
}

func main() {
	app := &cli.App{
		Name:    "BETZE: Benchmarking Data Exploration Tools with (Almost) Zero Effort",
		Usage:   "Creates explorative query loads for benchmarking based on the random explorer approach",
		Version: "v0.0.1",
		Authors: []*cli.Author{
			{
				Name:  "Nico Schäfer",
				Email: "nschaefer@cs.uni-kl.de",
			},
		},
		Before: initialize_cli,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "logfile",
				Usage: "The file to which the logs should be written to",
			},
		},
		EnableBashCompletion: true,
		Commands: []*cli.Command{
			fetch_datasets_command(),
			generate_queries_command(),
			translate_queries_command(),
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
