package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/JODA-Explore/BETZE/generator"
	"github.com/urfave/cli/v2"
)

func translate_queries_command() *cli.Command {
	flags := []cli.Flag{
		intermediate_flag(),
	}

	// For each language add the corresponding file flag
	lang_flags := get_language_flags()
	flags = append(flags, lang_flags...)

	return &cli.Command{
		Name:      "translate",
		Usage:     "Translates the internal query representation to the given languages.",
		ArgsUsage: "<betze.json>",
		Flags:     flags,
		Action:    translate_queries,
	}
}

func translate_queries(c *cli.Context) error {
	if c.NArg() == 0 {
		e := missingArgError{arg: "<betze.json>"}
		return &e
	}

	if c.Bool("intermediate-sets") && c.Bool("aggregate") {
		log.Fatalln("Cannot use aggregate and intermediate sets at the same time")
	}

	// Parse dataset file
	betze_file := c.Args().Get(0)
	f, err := os.Open(betze_file)
	if err != nil {
		return fmt.Errorf("could not open file: \"%v\"", err)
	}
	defer f.Close()
	byteValue, err := ioutil.ReadAll(f)
	if err != nil {
		return fmt.Errorf("could not read file: \"%v\"", err)
	}

	// Initialize predicate repository for translation
	predicateRepo := generator.GetPredicateFactoryRepo()
	predicateRepo.SetAll()

	// Initialize aggregation repository for translation
	aggregationRepo := generator.GetAggregationFactoryRepo()
	aggregationRepo.SetAll()

	// Parse queries
	queries, header, err := generator.UnmarshalQueries(byteValue)
	if err != nil {
		return fmt.Errorf("could not parse internal query file \"%v\"", err)
	}

	// Translate and serialize language specific queries
	err = translate_languages(queries, header, c)
	if err != nil {
		return err
	}

	return nil
}
