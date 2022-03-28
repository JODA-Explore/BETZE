package main

import (
	"fmt"
	"strings"

	"github.com/JODA-Explore/BETZE/languages"
	"github.com/urfave/cli/v2"
)

func joda_flag() *cli.StringFlag {
	return &cli.StringFlag{
		Name:  joda_host_opt,
		Usage: "The host of a JODA server instance. If provided, enables JODA communication support for the system.",
	}
}

func aggregate_flag() *cli.BoolFlag {
	return &cli.BoolFlag{
		Name:  "aggregate",
		Usage: "Removes most I/O from the queries which shifts comparison focus to internal systems. Not compatible with intermediate-sets",
	}

}

func aggregate_probability_flag() *cli.Float64Flag {
	return &cli.Float64Flag{
		Name:  "aggregation-probability",
		Value: 1.0,
		Usage: "The probability to perform an aggregation in the query. Only used if --aggregate is set",
	}
}

func intermediate_flag() *cli.BoolFlag {
	return &cli.BoolFlag{
		Name:  "intermediate-sets",
		Usage: "Create intermediate sets with which to continue query generation. If not set, all queries will only query the base datasets with increasingly complex queries",
	}
}

func get_language_flags() []cli.Flag {
	flags := []cli.Flag{}
	for _, lang := range languages.LanguageIndex() {
		flags = append(flags, &cli.StringFlag{
			Name:  fmt.Sprintf("%s-file", lang.ShortName()),
			Usage: fmt.Sprintf("Translates the queries to %s queries and stores them in this file.", lang.Name()),
		})
	}
	return flags
}

// Returns a list of possible configuration presets.
// The presets are "novice", "intermediate", and "expert"
func get_presets() []string {
	return []string{"novice", "intermediate", "expert"}
}

// Checks if the given preset is one of the possible configuration presets, ignoring case
func is_preset(preset string) bool {
	for _, p := range get_presets() {
		if strings.EqualFold(preset, p) {
			return true
		}
	}
	return false
}
