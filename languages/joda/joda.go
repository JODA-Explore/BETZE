package joda

import (
	"fmt"
	"log"
	"strings"

	"github.com/JODA-Explore/BETZE/query"
)

const (
	api_prefix = "/api/v2"
)

type Joda struct{}

// Attempts to connect to a JODA instance
func Connect(host string) (*JodaConnection, error) {
	connection := &JodaConnection{host: host}
	system, err := connection.System()
	if err != nil {
		return nil, err
	}
	log.Printf("Connected to JODA at %s with version %s", connection.host, system.Version.Version)
	return connection, nil
}

func (Joda) Translate(query query.Query) (query_string string) {
	// LOAD
	query_string += fmt.Sprintf("LOAD %s", query.BaseName())

	// TODO AS

	// CHOOSE
	filter := query.FilterPredicate()
	if filter != nil {
		query_string += fmt.Sprintf(" CHOOSE %s ", translate_predicate(filter))
	}

	var agg = query.Aggregation()
	if agg != nil {
		query_string += fmt.Sprintf(" AGG %s", translate_aggregation(agg))
	}

	// STORE
	if len(query.StoreName()) > 0 {
		query_string += fmt.Sprintf(" STORE %s", query.StoreName())
	}

	return
}

func (Joda) Name() string {
	return "JODA"
}

func (Joda) ShortName() string {
	return "joda"
}

func (Joda) Comment(comment string) string {
	return "# " + comment
}

func (Joda) Header() string {
	return ""
}

func (Joda) QueryDelimiter() string {
	return ""
}

func (Joda) SupportsIntermediate() bool {
	return true
}

func escape_string(str string) string {
	return strings.ReplaceAll(strings.ReplaceAll(str, "\\", "\\\\"), "\"", "\\\"")
}

func translate_predicate(predicate query.Predicate) string {
	switch v := predicate.(type) {
	case query.AndPredicate:
		return fmt.Sprintf("(%s && %s)", translate_predicate(v.Lhs), translate_predicate(v.Rhs))
	case query.OrPredicate:
		return fmt.Sprintf("(%s || %s)", translate_predicate(v.Lhs), translate_predicate(v.Rhs))
	case query.IntEqualityPredicate:
		return fmt.Sprintf("'%s' == %d", v.Path, v.Number)
	case query.FloatComparisonPredicate:
		var cmpstr = ">"
		if v.Smaller {
			cmpstr = "<"
		}
		if v.Equal {
			cmpstr += "="
		}
		return fmt.Sprintf("'%s' %s %f", v.Path, cmpstr, v.Number)
	case query.StrEqualityPredicate:
		return fmt.Sprintf("'%s' == \"%s\"", v.Path, v.Str)
	case query.StrPrefixPredicate:
		return fmt.Sprintf("STARTSWITH('%s',\"%s\")", v.Path, escape_string(v.Prefix))
	case query.ExistsPredicate:
		return fmt.Sprintf("EXISTS('%s')", v.Path)
	case query.IsStringPredicate:
		return fmt.Sprintf("ISSTRING('%s')", v.Path)
	case query.BoolEqualityPredicate:
		return fmt.Sprintf("'%s' == %t", v.Path, v.Value)
	case query.ObjectSizeComparisonPredicate:
		var cmpstr = ">"
		if v.Smaller {
			cmpstr = "<"
		}
		if v.Equal {
			cmpstr += "="
		}
		return fmt.Sprintf("ISOBJECT('%s') && MEMCOUNT('%s') %s %d", v.Path, v.Path, cmpstr, v.Number)
	case query.ArraySizeComparisonPredicate:
		var cmpstr = ">"
		if v.Smaller {
			cmpstr = "<"
		}
		if v.Equal {
			cmpstr += "="
		}
		return fmt.Sprintf("SIZE('%s') %s %d", v.Path, cmpstr, v.Number)
	default:
		log.Printf("Error: Missing predicate type translation: %s", predicate.String())
		return ""
	}

}

func translate_ungroupedaggregation(agg query.Aggregation) string {
	switch v := agg.(type) {
	case query.GlobalCountAggregation:
		return "COUNT('')"
	case query.CountAggregation:
		return fmt.Sprintf("COUNT('%s')", v.Path)
	case query.SumAggregation:
		return fmt.Sprintf("SUM('%s')", v.Path)
	default:
		log.Printf("Error: Missing aggregation type translation: %s", agg.String())
		return ""
	}

}

func translate_aggregation(agg query.Aggregation) string {

	var group, isgroup = agg.(query.GroupedAggregation)
	if isgroup { // If grouped aggregation, translate sub-aggregations
		return fmt.Sprintf("('': GROUP %s AS %s BY '%s')", translate_ungroupedaggregation(group.Agg), group.Name(), group.Path)
	} else {
		return fmt.Sprintf("('/%s': %s)", agg.Name(), translate_ungroupedaggregation(agg))
	}

}
