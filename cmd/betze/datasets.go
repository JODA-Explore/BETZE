package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/urfave/cli/v2"
)

func fetch_datasets_command() *cli.Command {
	return &cli.Command{
		Name:      "fetch-dataset",
		Usage:     "Fetches dataset(s) from the given source.",
		ArgsUsage: fmt.Sprintf("<provider %s> <sources ...>", dataset_providers),
		Flags: []cli.Flag{
			joda_flag(),
			&cli.StringFlag{
				Name:  "file",
				Usage: "A file to which the dataset(s) should be written",
				Value: "datasets.json",
			},
		},
		Action: fetch_datasets,
	}
}

func fetch_datasets(c *cli.Context) error {
	overall_start_time := time.Now()
	var sources []string
	for i := 1; i < c.NArg(); i++ {
		sources = append(sources, c.Args().Get(i))
	}
	switch strings.ToLower(c.Args().Get(0)) {
	case "":
		e := missingArgError{arg: "provider"}
		return &e
	case "joda":
		joda_con := joda_connect(c.String(joda_host_opt))
		datasets, err := joda_con.GetDatasets(sources)
		if err != nil {
			return fmt.Errorf("could not get datasets from JODA: %v", err)
		}
		b, err := json.Marshal(datasets)
		if err != nil {
			return fmt.Errorf("could not convert dataset to JSON: %v", err)
		}
		if filename := c.String("file"); len(filename) > 0 {
			f, err := os.Create(filename)
			if err != nil {
				return fmt.Errorf("could not create file: %v", err)
			}
			defer f.Close()
			f.Write(b)
			f.Sync()
		} else {
			fmt.Println(string(b))
		}
		fmt.Printf("Fetched %d dataset(s) in %v\n", len(datasets), time.Since(overall_start_time))
	default:
		exp := dataset_providers
		e := unknownArgValueError{arg: "provider", val: c.Args().Get(0), expected: &exp}
		return &e
	}

	return nil
}
