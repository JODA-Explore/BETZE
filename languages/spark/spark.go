package spark

import (
	"fmt"
	"log"
	"strings"

	"github.com/JODA-Explore/BETZE/query"
)

type Spark struct{}

func (Spark) Name() string {
	return "Spark"
}

func (Spark) ShortName() string {
	return "spark"
}

func (Spark) Comment(comment string) string {
	return "// " + comment
}

func (Spark) Header() string {
	return ""
}

func (Spark) QueryDelimiter() string {
	return ";"
}

func (Spark) SupportsIntermediate() bool {
	return true
}

func (Spark) Translate(query query.Query) (query_string string) {
	filter := query.FilterPredicate()
	agg := query.Aggregation()

	var stages []string

	// STORE
	if len(query.StoreName()) > 0 {
		query_string = fmt.Sprintf("val %s = ", query.StoreName())

	}

	// LOAD
	stages = append(stages, query.BaseName())

	// AGGREGATE (Select)
	if agg != nil {
		agg_str := translate_aggregation(agg)
		if len(agg_str) > 0 {
			filter_step := fmt.Sprintf("select(%s)", agg_str)
			stages = append(stages, filter_step)
		}
	}

	// CHOOSE
	if filter != nil {
		filter_step := fmt.Sprintf("where(%s)", translate_predicate(filter))
		stages = append(stages, filter_step)
	}

	// AGGREGATE (GroupBy)
	if agg != nil {
		agg_step := translate_group(agg)
		if len(agg_step) > 0 {
			stages = append(stages, agg_step)
		}
	}

	query_string += strings.Join(stages, ".")
	query_string += ".show()"

	return
}

func escape_string(str string) string {
	return strings.ReplaceAll(strings.ReplaceAll(str, "\\", "\\\\"), "\"", "\\\"")
}

func convert_path(path string) string {
	p := strings.ReplaceAll(path, "/", ".")
	if len(p) > 0 {
		return fmt.Sprintf("col(\"%s\")", p[1:])
	}
	return ""
}

func convert_path_subelements(path string) string {
	p := strings.ReplaceAll(path, "/", ".")
	if len(p) > 0 {
		return fmt.Sprintf("col(\"%s.*\")", p[1:])
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
	return fmt.Sprintf("(%s && %s)", lhs, rhs)
}

func translate_predicate(predicate query.Predicate) string {
	switch v := predicate.(type) {
	case query.AndPredicate:
		return translate_and_predicate(translate_predicate(v.Lhs), translate_predicate(v.Rhs))
	case query.OrPredicate:
		return fmt.Sprintf("(%s || %s)", translate_predicate(v.Lhs), translate_predicate(v.Rhs))
	case query.IntEqualityPredicate:
		return fmt.Sprintf("(%s === %d)", convert_path(v.Path), v.Number)
	case query.FloatComparisonPredicate:
		return fmt.Sprintf("(%s %s %f)", convert_path(v.Path), translate_cmp_operator(v.Smaller, v.Equal), v.Number)
	case query.StrEqualityPredicate:
		return fmt.Sprintf("(%s === %s)", convert_path(v.Path), v.Str)
	case query.StrPrefixPredicate:
		return fmt.Sprintf("(%s.startsWith(\"%s\"))", convert_path(v.Path), escape_string(v.Prefix))
	case query.ExistsPredicate:
		return fmt.Sprintf("(%s.isNotNull)", convert_path(v.Path))
	case query.IsStringPredicate:
		p := convert_path(v.Path)
		return fmt.Sprintf("not(%s === \"true\" || %s === \"false\" || %s.isNull || %s.cast(\"int\").isNotNull)", p, p, p, p)
	case query.BoolEqualityPredicate:
		return fmt.Sprintf("(%s === %t)", convert_path(v.Path), v.Value)
	case query.ObjectSizeComparisonPredicate:
		return fmt.Sprintf("size(array(%s)) %s %d", convert_path_subelements(v.Path), translate_cmp_operator(v.Smaller, v.Equal), v.Number)
	case query.ArraySizeComparisonPredicate:
		return fmt.Sprintf("size(%s) %s %d", convert_path(v.Path), translate_cmp_operator(v.Smaller, v.Equal), v.Number)
	default:
		log.Printf("Error: Missing predicate type translation: %s", predicate.String())
		return ""
	}

}

func translate_aggregation(agg query.Aggregation) (query_string string) {
	switch v := agg.(type) {
	case query.GlobalCountAggregation:
		return "count()"
	case query.CountAggregation:
		return fmt.Sprintf("count(%s)", convert_path(v.Path))
	case query.SumAggregation:
		return fmt.Sprintf("sum(%s)", convert_path(v.Path))
	default:
		return
	}
}

func translate_group(agg query.Aggregation) (query_string string) {
	var group, isgroup = agg.(query.GroupedAggregation)
	if isgroup { // If grouped aggregation, translate sub-aggregations
		return fmt.Sprintf("groupBy(%s).%s", convert_path(group.Path), translate_aggregation(group.Agg))
	}
	return
}
