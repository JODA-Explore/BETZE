package postgres

import (
	"fmt"
	"log"
	"strings"

	"github.com/JODA-Explore/BETZE/query"
)

type Postgres struct{}

func (Postgres) Name() string {
	return "PostgreSQL"
}

func (Postgres) ShortName() string {
	return "psql"
}

func (Postgres) Comment(comment string) string {
	return "-- " + comment
}

func (Postgres) Header() string {
	return ""
}

func (Postgres) QueryDelimiter() string {
	return ";"
}

func (Postgres) SupportsIntermediate() bool {
	return true
}

func (Postgres) Translate(query query.Query) (query_string string) {
	filter := query.FilterPredicate()

	// Command
	query_string += "SELECT"

	agg := query.Aggregation()
	if agg != nil {
		query_string += " " + translate_aggregation(agg)
	} else {
		query_string += " *"
	}

	// LOAD
	query_string += fmt.Sprintf(" FROM %s ", query.BaseName())

	// CHOOSE
	if filter != nil {
		filter_string := translate_predicate(filter)
		if agg != nil {
			agg_pred := translate_aggregation_prerequisite_predicate(agg)
			if agg_pred != "" {
				if filter_string != "" {
					filter_string = translate_and_predicate(filter_string, agg_pred)
				} else {
					filter_string = agg_pred
				}
			}
		}
		query_string += fmt.Sprintf(" WHERE %s ", filter_string)
	}

	if agg != nil {
		query_string += translate_group(agg)
	}

	// STORE
	if len(query.StoreName()) > 0 {
		query_string = fmt.Sprintf("CREATE TEMP TABLE %s AS %s; SELECT * FROM %s", query.StoreName(), query_string, query.StoreName())
	}

	return
}

func convert_path(path string) string {
	parts := strings.Split(path, "/")[1:]
	p := strings.Join(parts, ".")

	return fmt.Sprintf("$.%s", p)
}

func convert_extract_path(path string) string {
	parts := strings.Split(path, "/")[1:]
	p := strings.Join(parts, ",")

	return fmt.Sprintf("{%s}", p)
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

func escape_string(str string) string {
	return strings.ReplaceAll(strings.ReplaceAll(str, "\\", "\\\\"), "\"", "\\\"")
}

func translate_and_predicate(lhs string, rhs string) string {
	return fmt.Sprintf("( %s AND %s )", lhs, rhs)
}

func translate_predicate(predicate query.Predicate) (query_string string) {
	switch v := predicate.(type) {
	case query.AndPredicate:
		return translate_and_predicate(translate_predicate(v.Lhs), translate_predicate(v.Rhs))
	case query.OrPredicate:
		return fmt.Sprintf("( %s OR %s )", translate_predicate(v.Lhs), translate_predicate(v.Rhs))
	case query.IntEqualityPredicate:
		return fmt.Sprintf("jsonb_path_exists(doc,'%s ? (@ == %d)')", convert_path(v.Path), v.Number)
	case query.FloatComparisonPredicate:
		return fmt.Sprintf("jsonb_path_exists(doc,'%s ? (@ %s %f)')", convert_path(v.Path), translate_cmp_operator(v.Smaller, v.Equal), v.Number)
	case query.StrEqualityPredicate:
		return fmt.Sprintf("jsonb_path_exists(doc,'%s ? (@ == %s)')", convert_path(v.Path), v.Str)
	case query.StrPrefixPredicate:
		return fmt.Sprintf("jsonb_path_exists(doc,'%s ? (@ starts with \"%s\")')", convert_path(v.Path), escape_string(v.Prefix))
	case query.ExistsPredicate:
		return fmt.Sprintf("jsonb_path_exists(doc,'%s')", convert_path(v.Path))
	case query.IsStringPredicate:
		return fmt.Sprintf("jsonb_path_exists(doc,'%s.type() ? (@ == \"string\")')", convert_path(v.Path))
	case query.BoolEqualityPredicate:
		return fmt.Sprintf("jsonb_path_exists(doc,'%s ? (@ == %t)')", convert_path(v.Path), v.Value)
	case query.ObjectSizeComparisonPredicate:
		return fmt.Sprintf("(jsonb_path_exists(doc,'%s ? (@.type() == \"object\")') AND jsonb_path_exists(jsonb_path_query_array(doc, '%s.keyvalue().key'),'$.size() ? (@ %s %d)'))", convert_path(v.Path), convert_path(v.Path), translate_cmp_operator(v.Smaller, v.Equal), v.Number)
	case query.ArraySizeComparisonPredicate:
		return fmt.Sprintf("jsonb_path_exists(doc,'%s ? (@.type() == \"array\" && @.size() %s %d)') ", convert_path(v.Path), translate_cmp_operator(v.Smaller, v.Equal), v.Number)
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
		return fmt.Sprintf("jsonb_path_exists(doc,'%s.type() ? (@ == \"number\")')", convert_path(v.Path))
	default:
		// No prerequisite
		return ""
	}
}

func translate_aggregation(agg query.Aggregation) (query_string string) {
	switch v := agg.(type) {
	case query.GroupedAggregation:
		return fmt.Sprintf("doc #> '%s' as group, %s", convert_extract_path(v.Path), translate_aggregation(v.Agg))
	case query.GlobalCountAggregation:
		return "COUNT(*)"
	case query.CountAggregation:
		return fmt.Sprintf("COUNT(doc #> '%s')", convert_extract_path(v.Path))
	case query.SumAggregation:
		return fmt.Sprintf("SUM((doc #>> '%s')::float)", convert_extract_path(v.Path))
	default:
		log.Printf("Error: Missing aggregation type translation: %s", agg.String())
	}
	return
}

func translate_group(agg query.Aggregation) (query_string string) {
	switch v := agg.(type) {
	case query.GroupedAggregation:
		return fmt.Sprintf(" GROUP BY doc #> '%s'", convert_extract_path(v.Path))
	default:
		return ""
	}
}
