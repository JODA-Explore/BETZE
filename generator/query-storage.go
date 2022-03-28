package generator

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/JODA-Explore/BETZE/dataset"
	"github.com/JODA-Explore/BETZE/query"
)

////////////////////////////////////////////////////////////////////////////////
// 	JSON Marshaling
////////////////////////////////////////////////////////////////////////////////

type predContainer struct {
	Type  string          `json:"type"`
	Value query.Predicate `json:"parameter"`
}

type boolPredContainer struct {
	Lhs json.RawMessage `json:"Lhs"`
	Rhs json.RawMessage `json:"Rhs"`
}
type predRawContainer struct {
	Type  string            `json:"type"`
	Value boolPredContainer `json:"parameter"`
}

// Marshal a predicate to a re-parsable JSON object
func MarshalPredicate(pred query.Predicate) ([]byte, error) {
	t := reflect.TypeOf(pred)

	// Check And/Or, as they don't have factories
	if t == reflect.TypeOf(query.AndPredicate{}) {
		lhs, err := MarshalPredicate(pred.(query.AndPredicate).Lhs)
		if err != nil {
			return nil, err
		}
		rhs, err := MarshalPredicate(pred.(query.AndPredicate).Rhs)
		if err != nil {
			return nil, err
		}
		return json.Marshal(predRawContainer{
			Type: "AndPredicate",
			Value: boolPredContainer{
				Lhs: lhs,
				Rhs: rhs,
			},
		})
	}
	if t == reflect.TypeOf(query.OrPredicate{}) {
		lhs, err := MarshalPredicate(pred.(query.OrPredicate).Lhs)
		if err != nil {
			return nil, err
		}
		rhs, err := MarshalPredicate(pred.(query.OrPredicate).Rhs)
		if err != nil {
			return nil, err
		}
		return json.Marshal(predRawContainer{
			Type: "OrPredicate",
			Value: boolPredContainer{
				Lhs: lhs,
				Rhs: rhs,
			},
		})
	}

	//Check all factories
	for _, factory := range GetPredicateFactoryRepo().GetAll() {
		if factory.Type() == t {
			return json.Marshal(predContainer{
				Type:  factory.ID(),
				Value: pred,
			})
		}
	}
	return nil, fmt.Errorf("no factory found for predicate type %s", reflect.TypeOf(pred))
}

// Unmarshal a predicate to a re-parsable JSON object
func UnmarshalPredicate(data []byte) (query.Predicate, error) {
	m := map[string]interface{}{}
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}

	customTypes := map[string]reflect.Type{}
	for _, factory := range GetPredicateFactoryRepo().GetAll() {
		customTypes[factory.ID()] = factory.Type()
	}

	typeName := m["type"].(string)

	if typeName == "AndPredicate" || typeName == "OrPredicate" {
		valueBytes, err := json.Marshal(m["parameter"])
		if err != nil {
			return nil, err
		}
		value := boolPredContainer{}
		if err := json.Unmarshal(valueBytes, &value); err != nil {
			return nil, err
		}
		lhs, err := UnmarshalPredicate(value.Lhs)
		if err != nil {
			return nil, err
		}
		rhs, err := UnmarshalPredicate(value.Rhs)
		if err != nil {
			return nil, err
		}
		if typeName == "AndPredicate" {
			return query.AndPredicate{
				Lhs: lhs,
				Rhs: rhs,
			}, nil
		}
		return query.OrPredicate{
			Lhs: lhs,
			Rhs: rhs,
		}, nil
	}

	var value reflect.Value
	var predicate query.Predicate
	if ty, found := customTypes[typeName]; found {
		value = reflect.New(ty)
		predicate = value.Interface().(query.Predicate)
	}

	valueBytes, err := json.Marshal(m["parameter"])
	if err != nil {
		return nil, err
	}

	if err = json.Unmarshal(valueBytes, &predicate); err != nil {
		return nil, err
	}

	return value.Elem().Interface().(query.Predicate), nil
}

type aggContainer struct {
	Type  string          `json:"type"`
	Value json.RawMessage `json:"parameter"`
}

type groupedAggContainer struct {
	Path   string       `json:"path"`
	SubAgg aggContainer `json:"subAggregation"`
}

// Marshal a aggregation to a re-parsable JSON object
func MarshalAggregation(agg query.Aggregation) (*aggContainer, error) {
	if agg == nil {
		return nil, nil
	}
	t := reflect.TypeOf(agg)
	var group, isgroup = (agg).(query.GroupedAggregation)
	if isgroup { // If group, special nested handling
		// Marshal subaggregate
		subagg, err := MarshalAggregation(group.Agg)
		if err != nil {
			return nil, err
		}
		// Create data container
		groupCont := groupedAggContainer{
			Path:   group.Path,
			SubAgg: *subagg,
		}
		// Marshal data container
		b, err := json.Marshal(groupCont)
		if err != nil {
			return nil, err
		}
		// Return aggregation container
		return &aggContainer{
			Type:  GroupByAggregationFactory{}.ID(),
			Value: b,
		}, nil
	} else {
		//Check all factories
		for _, factory := range GetAggregationFactoryRepo().GetAll() {
			if factory.Type() == t {
				b, err := json.Marshal(agg)
				if err != nil {
					return nil, err
				}
				return &aggContainer{
					Type:  factory.ID(),
					Value: b,
				}, nil
			}
		}
		return nil, fmt.Errorf("no factory found for aggregation type %s", t)
	}

}

// Unmarshal a aggregation to a re-parsable JSON object
func UnmarshalAggregation(data aggContainer) (query.Aggregation, error) {

	customTypes := map[string]reflect.Type{}
	for _, factory := range GetAggregationFactoryRepo().GetAll() {
		customTypes[factory.ID()] = factory.Type()
	}
	typeName := data.Type
	groupId := GroupByAggregationFactory{}.ID()

	if typeName == groupId { // Group special nested handling
		var aggCont groupedAggContainer
		if err := json.Unmarshal(data.Value, &aggCont); err != nil {
			return nil, err
		}
		// Unmarshal subaggregate
		subagg, err := UnmarshalAggregation(aggCont.SubAgg)
		if err != nil {
			return nil, err
		}

		// Create group
		group := query.GroupedAggregation{
			Path: aggCont.Path,
			Agg:  subagg,
		}
		return group, nil

	} else {
		var value reflect.Value
		var aggregation query.Aggregation
		if ty, found := customTypes[typeName]; found {
			value = reflect.New(ty)
			aggregation = value.Interface().(query.Aggregation)
		}

		if err := json.Unmarshal(data.Value, &aggregation); err != nil {
			return nil, err
		}

		return value.Elem().Interface().(query.Aggregation), nil
	}
}

type queryJSON struct {
	Base      string          `json:"load"`
	Filter    json.RawMessage `json:"filter"`
	Aggregate *aggContainer   `json:"agg"`
	Store     string          `json:"store"`
}

func MarshalQuery(q query.Query) ([]byte, error) {
	m_pred, err := MarshalPredicate(q.FilterPredicate())
	if err != nil {
		return nil, err
	}

	m_agg, err := MarshalAggregation(q.Aggregation())
	if err != nil {
		return nil, err
	}

	return json.Marshal(queryJSON{
		q.BaseName(),
		m_pred,
		m_agg,
		q.StoreName(),
	})
}

func UnmarshalQuery(b []byte) (*query.Query, error) {
	temp := &queryJSON{}

	if err := json.Unmarshal(b, &temp); err != nil {
		return nil, err
	}
	query := query.Query{}
	query.Load(&dataset.DataSet{Name: temp.Base}).Store(temp.Store)

	pred, err := UnmarshalPredicate(temp.Filter)
	if err != nil {
		return nil, err
	}
	query.Filter(pred)

	if temp.Aggregate != nil {
		agg, err := UnmarshalAggregation(*temp.Aggregate)
		if err != nil {
			return nil, err
		}
		query.Aggregate(agg)
	}

	return &query, nil
}

type queriesJSON struct {
	Config  string            `json:"config"`
	Queries []json.RawMessage `json:"queries"`
}

func MarshalQueries(q []query.Query, config string) ([]byte, error) {
	queries := queriesJSON{
		Config:  config,
		Queries: make([]json.RawMessage, 0, len(q)),
	}
	for _, query := range q {
		m_query, err := MarshalQuery(query)
		if err != nil {
			return nil, err
		}
		queries.Queries = append(queries.Queries, m_query)
	}
	return json.Marshal(queries)
}

func UnmarshalQueries(b []byte) ([]query.Query, string, error) {
	temp := queriesJSON{}
	if err := json.Unmarshal(b, &temp); err != nil {
		return nil, "", err
	}
	queries := make([]query.Query, 0, len(temp.Queries))
	for _, query := range temp.Queries {
		q, err := UnmarshalQuery(query)
		if err != nil {
			return nil, "", err
		}
		queries = append(queries, *q)
	}
	return queries, temp.Config, nil
}
