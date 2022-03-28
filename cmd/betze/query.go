package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/JODA-Explore/BETZE/dataset"
	"github.com/JODA-Explore/BETZE/generator"
	"github.com/JODA-Explore/BETZE/query"

	"github.com/urfave/cli/v2"
)

func generate_queries_command() *cli.Command {
	flags := []cli.Flag{
		&cli.Int64Flag{
			Name:        "seed",
			DefaultText: "current timestamp",
			Value:       time.Now().UnixNano(),
			Usage:       "The seed to use for the random number generator",
		},
		&cli.Float64Flag{
			Name:  "min-selectivity",
			Value: 0.2,
			Usage: "The minimum selectivity of a query",
		},
		&cli.Float64Flag{
			Name:  "max-selectivity",
			Value: 0.9,
			Usage: "The maximum selectivity of a query",
		},
		&cli.Float64Flag{
			Name:        "probability-backtrack",
			Value:       -1,
			Usage:       "The probability to backtrack to the previous dataset",
			DefaultText: "0.4",
		},
		&cli.Float64Flag{
			Name:        "probability-randomjump",
			Value:       -1,
			Usage:       "The probability to randomly jump to another node",
			DefaultText: "0.1",
		},
		&cli.Int64Flag{
			Name:        "num_queries",
			Value:       -1,
			Usage:       "Number of queries to generate",
			DefaultText: "10",
		},
		intermediate_flag(),
		aggregate_flag(),
		aggregate_probability_flag(),
		&cli.StringSliceFlag{
			Name:    "include-aggregation",
			Aliases: []string{"a"},
			Usage:   "Use the aggregations specified here for random query generation. If none are given, all aggregations are used.",
		},
		&cli.StringSliceFlag{
			Name:  "exclude-aggregation",
			Usage: "Excludes the aggregations specified here from random query generation.",
		},
		&cli.BoolFlag{
			Name:  "weighted-paths",
			Usage: "Choose the random path to generate a predicate for by inverse path-depth weight",
		},
		&cli.StringSliceFlag{
			Name:    "include-predicate",
			Aliases: []string{"p"},
			Usage:   "Use the predicates specified here for random query generation. If none are given, all predicates are used.",
		},
		&cli.StringSliceFlag{
			Name:  "exclude-predicate",
			Usage: "Excludes the predicates specified here from random query generation.",
		},
		&cli.StringFlag{
			Name:  "preset",
			Usage: fmt.Sprintf("The user session preset. Explicitely set options override the preset options. Available presets are: %v", get_presets()),
			Value: "intermediate",
		},
		&cli.StringFlag{
			Name:  "betze-file",
			Usage: "File to store the internal query representation. Can be used to translate already generated queries.",
			Value: "betze.json",
		},
		joda_flag(),
	}

	// For each language add the corresponding file flag
	lang_flags := get_language_flags()
	flags = append(flags, lang_flags...)

	return &cli.Command{
		Name:      "generate",
		Usage:     "Generates a query set with the given parameters. If support for a server is enabled, it is used to verify the generated queries.",
		ArgsUsage: "<dataset.json>",
		Flags:     flags,
		Action:    generate_queries,
	}
}

func generate_queries(c *cli.Context) error {
	overall_start_time := time.Now()
	if c.NArg() == 0 {
		e := missingArgError{arg: "dataset"}
		return &e
	}

	if c.Bool("intermediate-sets") && c.Bool("aggregate") {
		log.Fatalln("Cannot use aggregate and intermediate sets at the same time")
	}

	// Parse dataset file
	dataset_file := c.Args().Get(0)
	f, err := os.Open(dataset_file)
	if err != nil {
		return fmt.Errorf("could not open file: \"%v\"", err)
	}
	defer f.Close()
	byteValue, err := ioutil.ReadAll(f)
	if err != nil {
		return fmt.Errorf("could not read file: \"%v\"", err)
	}

	var datasets []dataset.DataSet

	err = json.Unmarshal(byteValue, &datasets)
	if err != nil {
		return fmt.Errorf("could not parse dataset file: \"%v\"", err)
	}

	if !is_preset(c.String("preset")) {
		return fmt.Errorf("`%s` is not a valid preset. Available presets are: %v", c.String("preset"), get_presets())
	}

	// Get arguments
	minSelectivity := c.Float64("min-selectivity")
	maxSelectivity := c.Float64("max-selectivity")
	seed := c.Int64("seed")
	num_queries := c.Int64("num_queries")
	if num_queries < 0 {
		switch strings.ToLower(c.String("preset")) {
		case "novice":
			num_queries = 20
		case "intermediate":
			num_queries = 10
		case "expert":
			num_queries = 5
		default: // Should not happen
			num_queries = 10
		}
	}

	predicateRepo := generator.GetPredicateFactoryRepo()
	include_preds := c.StringSlice("include-predicate")
	if len(include_preds) > 0 {
		for _, pred := range include_preds {
			err := predicateRepo.Include(pred)
			if err != nil {
				log.Fatalf("%s, must be one of: '%s'", err, strings.Join(predicateRepo.GetAllIDs(), ","))
			}
		}
	} else {
		predicateRepo.SetDefault()
	}

	exclude_preds := c.StringSlice("exclude-predicate")
	if len(exclude_preds) > 0 {
		for _, pred := range exclude_preds {
			predicateRepo.Exclude(pred)
		}
	}

	aggregationRepo := generator.GetAggregationFactoryRepo()
	include_aggs := c.StringSlice("include-aggregation")
	if len(include_aggs) > 0 {
		for _, agg := range include_aggs {
			err := aggregationRepo.Include(agg)
			if err != nil {
				log.Fatalf("%s, must be one of: '%s'", err, strings.Join(aggregationRepo.GetAllIDs(), ","))
			}
		}
	} else {
		aggregationRepo.SetDefault()
	}

	exclude_aggs := c.StringSlice("exclude-aggregation")
	if len(exclude_aggs) > 0 {
		for _, agg := range exclude_aggs {
			aggregationRepo.Exclude(agg)
		}
	}

	// Generate queries
	query_generator := generator.New(seed)
	query_generator.MinSelectivity = minSelectivity
	query_generator.MaxSelectivity = maxSelectivity
	query_generator.RandomBrowseProb = c.Float64("probability-randomjump")
	if query_generator.RandomBrowseProb < 0 {
		switch strings.ToLower(c.String("preset")) {
		case "novice":
			query_generator.RandomBrowseProb = 0.3
		case "intermediate":
			query_generator.RandomBrowseProb = 0.1
		case "expert":
			query_generator.RandomBrowseProb = 0.05
		default: // Should not happen
			query_generator.RandomBrowseProb = 0.1
		}
	}
	query_generator.GoBackProb = c.Float64("probability-backtrack")
	if query_generator.GoBackProb < 0 {
		switch strings.ToLower(c.String("preset")) {
		case "novice":
			query_generator.GoBackProb = 0.5
		case "intermediate":
			query_generator.GoBackProb = 0.4
		case "expert":
			query_generator.GoBackProb = 0.2
		default: // Should not happen
			query_generator.GoBackProb = 0.4
		}
	}
	query_generator.Predicates = predicateRepo.GetChosen()
	if c.Bool("aggregate") {
		query_generator.Aggregations = aggregationRepo.GetChosen()
	}
	query_generator.AggregationProb = c.Float64("aggregation-probability")
	query_generator.WeightedPaths = c.Bool("weighted-paths")

	var queries []query.Query
	joda_con := joda_connect(c.String(joda_host_opt))
	if joda_con != nil {
		queries, err = query_generator.GenerateQuerySetWithJoda(datasets, num_queries, *joda_con)
		if err != nil {
			return err
		}
	} else {
		queries = query_generator.GenerateQuerySet(datasets, num_queries)
	}

	//Common header for all query files
	header := fmt.Sprintf("Created with %s (version %s), seed %d (%s)", c.App.Name, c.App.Version, seed, query_generator.PrintConfig())

	// Serialize internal queries
	betze_bytes, err := generator.MarshalQueries(queries, header)
	if err != nil {
		return fmt.Errorf("can't serialize internal queries: %v", err)
	}

	betze_f, err := os.Create(c.String("betze-file"))
	if err != nil {
		return fmt.Errorf("could not create file for writing internal queries: %v", err)
	}
	betze_f.Write(betze_bytes)
	betze_f.Close()

	log.Printf("Generated %d queries in %s\n", len(queries), time.Since(overall_start_time))

	// Display queries
	fmt.Println(header)
	fmt.Println("----------------------")
	for _, query := range queries {
		fmt.Println(query.String())
		fmt.Println("----------------------")
	}

	// Translate and serialize language specific queries
	err = translate_languages(queries, header, c)
	if err != nil {
		return err
	}

	return nil
}
