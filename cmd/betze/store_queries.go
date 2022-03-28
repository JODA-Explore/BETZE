package main

import (
	"fmt"
	"os"

	"github.com/JODA-Explore/BETZE/languages"
	"github.com/JODA-Explore/BETZE/query"
	"github.com/urfave/cli/v2"
)

func store_queries(queries []query.Query, filename string, header string, language languages.Language) error {
	if len(filename) > 0 {
		f, err := os.Create(filename)
		if err != nil {
			return fmt.Errorf("could not create file for writing %s queries: %v", language.Name(), err)
		}
		defer f.Close()

		f.Write([]byte(language.Header()))

		// Write header
		f.Write([]byte(language.Comment(header) + "\n"))

		for _, query := range queries {
			translated_query := language.Translate(query)
			f.Write([]byte(translated_query))
			f.Write([]byte(language.QueryDelimiter()))
			f.Write([]byte("\n"))
		}

		f.Sync()
	}
	return nil
}

// Translate the given queries to all specified languages and store in query file
func translate_languages(queries []query.Query, header string, c *cli.Context) error {

	// Split list of languages into ones that support intermediate sets and ones who don't
	var intermediate_language []languages.Language
	var non_intermediate_language []languages.Language
	for _, lang := range languages.LanguageIndex() {
		if c.Bool("intermediate-sets") && lang.SupportsIntermediate() {
			intermediate_language = append(intermediate_language, lang)
		} else {
			non_intermediate_language = append(non_intermediate_language, lang)
		}
	}

	// Write queries for languages with intermediate sets to files
	for _, language := range intermediate_language {
		err := store_queries(queries, c.String(fmt.Sprintf("%s-file", language.ShortName())), header, language)
		if err != nil {
			return fmt.Errorf("could not write %s queries: %v", language.Name(), err)
		}
	}
	// Remove intermediate sets and write queries for languages with no intermediate sets to files
	queries = query.RemoveIntermediateSets(queries)
	for _, language := range non_intermediate_language {
		err := store_queries(queries, c.String(fmt.Sprintf("%s-file", language.ShortName())), header, language)
		if err != nil {
			return fmt.Errorf("could not write %s queries: %v", language.Name(), err)
		}
	}
	return nil
}
