package jq

import (
	"fmt"
	"log"
	"strings"

	"github.com/JODA-Explore/BETZE/query"
)

type Jq struct{}

func (Jq) Name() string {
	return "jq"
}

func (Jq) ShortName() string {
	return "jq"
}

func (Jq) Comment(comment string) string {
	return "# " + comment
}

func (Jq) Header() string {
	return "#!/bin/sh\n\n"
}

func (Jq) QueryDelimiter() string {
	return ""
}

func (Jq) SupportsIntermediate() bool {
	return true
}

func (Jq) Translate(query query.Query) (query_string string) {
	filter := query.FilterPredicate()
	agg := query.Aggregation()

	// Start JQ commant
	query_string += "jq -c '"

	// Define aggregation function
	agg_func := ""
	if agg != nil {
		agg_func = fmt.Sprintf("def agg(s): reduce s as %s; ", translate_aggregation(agg))
	}

	if !query.AggregationIsGrouped() {
		// If aggregation happens in main invokation, define function there
		query_string += agg_func
	}

	// Start piping "inputs" stream
	inner_statement := "inputs"
	var pred string
	if filter != nil {
		pred = translate_predicate(filter)
		if agg != nil {
			agg_pred := translate_aggregation_prerequisite_predicate(agg)
			if agg_pred != "" {
				if pred != "" {
					pred = translate_and_predicate(pred, agg_pred)
				} else {
					pred = agg_pred
				}
			}
		}
		// Pipe stream to select
		inner_statement += fmt.Sprintf(" | select(%s)", pred)
	}

	if agg != nil && !query.AggregationIsGrouped() {
		query_string += translate_group(agg)
		// query = "agg(<stream>)"
		query_string += fmt.Sprintf("agg(%s)", inner_statement)
	} else {
		// query = "<stream>"
		query_string += inner_statement
	}

	query_string += "'" // Close JQ command

	// LOAD
	query_string += fmt.Sprintf(" %s.json", query.BaseName())

	if query.AggregationIsGrouped() {
		// If additional group is required
		// Start group and aggregate query
		// query = jq -c '<stream>' | jq -s -c 'group_by(.key) | agg(<group>)'
		query_string += fmt.Sprintf(" | jq -s -c '%s %s'", agg_func, translate_group(agg))
	}

	// STORE
	if len(query.StoreName()) > 0 {
		query_string += fmt.Sprintf(" > %s.json", query.StoreName())
	}

	return
}

func convert_path(path string) string {
	parts := strings.Split(path, "/")[1:]
	p := strings.Join(parts, ".")

	return "." + p
}

func parent_path(path string) string {
	parts := strings.Split(path, "/")[1:]

	if len(parts) > 0 {
		return "." + strings.Join(parts[:len(parts)-1], ".")
	}
	return "."
}

func last_key(path string) string {
	parts := strings.Split(path, "/")[1:]

	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return ""
}

func translate_cmp_operator(smaller bool, equal bool) string {
	var cmpstr = ">"
	if smaller {
		cmpstr = "<"
	}
	if equal {
		cmpstr += "="
	}
	return cmpstr
}

func translate_and_predicate(lhs string, rhs string) string {
	return fmt.Sprintf("( %s and %s )", lhs, rhs)
}

func escape_string(str string) string {
	return strings.ReplaceAll(strings.ReplaceAll(str, "\\", "\\\\"), "\"", "\\\"")
}

func translate_predicate(predicate query.Predicate) (query_string string) {
	switch v := predicate.(type) {
	case query.AndPredicate:
		return translate_and_predicate(translate_predicate(v.Lhs), translate_predicate(v.Rhs))
	case query.OrPredicate:
		return fmt.Sprintf("( %s or %s )", translate_predicate(v.Lhs), translate_predicate(v.Rhs))
	case query.IntEqualityPredicate:
		return fmt.Sprintf("( %s == %d )", convert_path(v.Path), v.Number)
	case query.FloatComparisonPredicate:
		return fmt.Sprintf("( %s %s %f )", convert_path(v.Path), translate_cmp_operator(v.Smaller, v.Equal), v.Number)
	case query.StrEqualityPredicate:
		return fmt.Sprintf("( %s == %s )", convert_path(v.Path), v.Str)
	case query.StrPrefixPredicate:
		return fmt.Sprintf("( %s | (. != null and startswith(\"%s\")) )", convert_path(v.Path), escape_string(v.Prefix))
	case query.ExistsPredicate:
		return fmt.Sprintf("( %s | has(\"%s\") )", parent_path(v.Path), last_key(v.Path))
	case query.IsStringPredicate:
		return fmt.Sprintf("( %s | type == \"string\" )", convert_path(v.Path))
	case query.BoolEqualityPredicate:
		return fmt.Sprintf("( %s == %t )", convert_path(v.Path), v.Value)
	case query.ObjectSizeComparisonPredicate:
		return fmt.Sprintf("( %s | ((type == \"object\") and (keys | length %s %d)) )", convert_path(v.Path), translate_cmp_operator(v.Smaller, v.Equal), v.Number)
	case query.ArraySizeComparisonPredicate:
		return fmt.Sprintf("( %s | length %s %d )", convert_path(v.Path), translate_cmp_operator(v.Smaller, v.Equal), v.Number)
	default:
		log.Printf("Error: Missing predicate type translation: %s", predicate.String())
	}

	return
}

func translate_aggregation_prerequisite_predicate(agg query.Aggregation) string {
	switch v := agg.(type) {
	case query.GroupedAggregation:
		return translate_aggregation_prerequisite_predicate(v.Agg)
	case query.SumAggregation:
		return fmt.Sprintf("( %s | type == \"number\" )", convert_path(v.Path))
	default:
		// No prerequisite
		return ""
	}
}

func translate_aggregation(agg query.Aggregation) string {
	switch v := agg.(type) {
	case query.GroupedAggregation:
		return translate_aggregation(v.Agg)
	case query.GlobalCountAggregation:
		return "$x (0; . + 1)"
	case query.CountAggregation:
		return fmt.Sprintf("$x (0; . + ($x | %s | 1))", convert_path(v.Path))
	case query.SumAggregation:
		return fmt.Sprintf("$x (0; . + ($x | %s))", convert_path(v.Path))
	default:
		log.Printf("Error: Missing aggregation type translation: %s", agg.String())
		return ""
	}
}

func translate_group(agg query.Aggregation) string {
	switch v := agg.(type) {
	case query.GroupedAggregation:
		return fmt.Sprintf("group_by(%s) | map({group: .[0]%s,  %s: agg(.[])})", convert_path(v.Path), convert_path(v.Path), v.Name())
	default:
		return ""
	}
}
