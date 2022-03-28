package mongodb

import (
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/JODA-Explore/BETZE/query"
)

type MongoDB struct{}

func (MongoDB) Name() string {
	return "MongoDB"
}

func (MongoDB) ShortName() string {
	return "mongo"
}

func (MongoDB) Comment(comment string) string {
	return "// " + comment
}

func (MongoDB) Header() string {
	return ""
}

func (MongoDB) QueryDelimiter() string {
	return ";"
}

func (MongoDB) SupportsIntermediate() bool {
	return true
}

func (MongoDB) Translate(query query.Query) (query_string string) {
	filter := query.FilterPredicate()
	agg := query.Aggregation()

	// LOAD
	query_string += fmt.Sprintf("db.%s", query.BaseName())

	var stages []string

	// CHOOSE
	if filter != nil {
		filter_step := fmt.Sprintf("{ $match : %s }", translate_predicate(filter))
		stages = append(stages, filter_step)
	}

	// AGGREGATE
	if agg != nil {
		agg_step := translate_aggregation(agg)
		stages = append(stages, agg_step)
	}

	// STORE
	if len(query.StoreName()) > 0 {
		store_step := fmt.Sprintf("{ $out : \"%s\" }", query.StoreName())
		stages = append(stages, store_step)
	}

	query_string += fmt.Sprintf(".aggregate([%s])", strings.Join(stages, ", "))

	return
}

func convert_path(path string) string {
	p := strings.ReplaceAll(path, "/", ".")
	if len(p) > 0 {
		return p[1:]
	}
	return ""
}

func convert_path_replace_root(path string) string {
	ret := convert_path(path)
	if len(ret) == 0 {
		return "$ROOT"
	}
	return ret
}

func predicate_at_path(path string, predicate string) string {
	return fmt.Sprintf("{\"%s\" : %s}", convert_path(path), predicate)
}

func translate_cmp_function(smaller bool, equal bool) string {
	var cmpstr = "$gt"
	if smaller {
		cmpstr = "$lt"
	}
	if equal {
		cmpstr += "e"
	}
	return cmpstr
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
	return fmt.Sprintf("{ $and: [ %s , %s ] }", lhs, rhs)
}

func translate_predicate(predicate query.Predicate) string {
	switch v := predicate.(type) {
	case query.AndPredicate:
		return translate_and_predicate(translate_predicate(v.Lhs), translate_predicate(v.Rhs))
	case query.OrPredicate:
		return fmt.Sprintf("{ $or: [ %s , %s ] }", translate_predicate(v.Lhs), translate_predicate(v.Rhs))
	case query.IntEqualityPredicate:
		return fmt.Sprintf("{\"%s\" : %d}", convert_path(v.Path), v.Number)
	case query.FloatComparisonPredicate:
		return predicate_at_path(v.Path, fmt.Sprintf("{%s: %f}", translate_cmp_function(v.Smaller, v.Equal), v.Number))
	case query.StrEqualityPredicate:
		return fmt.Sprintf("{\"%s\" : \"%s\"}", convert_path(v.Path), v.Str)
	case query.StrPrefixPredicate:
		return fmt.Sprintf("{\"%s\": /^%s.*/}", convert_path(v.Path), strings.ReplaceAll(regexp.QuoteMeta(v.Prefix), "/", "\\/"))
	case query.ExistsPredicate:
		return predicate_at_path(v.Path, "{ $exists: true }")
	case query.IsStringPredicate:
		return predicate_at_path(v.Path, "{ $type: \"string\" }")
	case query.BoolEqualityPredicate:
		return fmt.Sprintf("{\"%s\" : %t}", convert_path(v.Path), v.Value)
	case query.ObjectSizeComparisonPredicate:
		isObject := predicate_at_path(v.Path, "{$type : \"object\"}")
		objSize := fmt.Sprintf("{$expr:{%s:[{$size:{\"$objectToArray\" : \"$%s\"}}, %d]}}", translate_cmp_function(v.Smaller, v.Equal), convert_path_replace_root(v.Path), v.Number)
		return translate_and_predicate(isObject, objSize)
	case query.ArraySizeComparisonPredicate:
		isArray := predicate_at_path(v.Path, "{$type : \"array\"}")
		arrSize := fmt.Sprintf("{$expr:{%s:[{$size:\"$%s\"}, %d]}}", translate_cmp_function(v.Smaller, v.Equal), convert_path_replace_root(v.Path), v.Number)
		return translate_and_predicate(isArray, arrSize)
	default:
		log.Printf("Error: Missing predicate type translation: %s", predicate.String())
		return ""
	}

}

func translate_ungroupedaggregation(agg query.Aggregation) string {
	switch v := agg.(type) {
	case query.GlobalCountAggregation:
		return fmt.Sprintf("%s: { $sum: 1 }", v.Name())
	case query.CountAggregation:
		return fmt.Sprintf("%s: { $sum: {\"$cond\": [ { \"$ifNull\": [\"$%s\", false] }, 1, 0 ]} }", v.Name(), convert_path_replace_root(v.Path))
	case query.SumAggregation:
		return fmt.Sprintf("%s: { $sum: \"$%s\"}", v.Name(), convert_path_replace_root(v.Path))
	default:
		log.Printf("Error: Missing aggregation type translation: %s", agg.String())
		return ""
	}
}

func translate_aggregation(agg query.Aggregation) string {
	var group_id = "null"
	var agg_string = ""

	var group, isgroup = agg.(query.GroupedAggregation)
	if isgroup { // If grouped aggregation, translate sub-aggregations
		group_id = fmt.Sprintf("'$%s'", convert_path(group.Path))
		agg_string = translate_ungroupedaggregation(group.Agg)
	} else {
		agg_string = translate_ungroupedaggregation(agg)
	}

	return fmt.Sprintf("{ $group: { _id: %s, %s } }", group_id, agg_string)
}
