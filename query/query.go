package query

import (
	"fmt"

	"github.com/JODA-Explore/BETZE/dataset"
)

// Query struct, specifying how the queries are constructed.
type Query struct {
	// The dataset to load from
	baseDataset *dataset.DataSet
	// The name/ID of the dataset to be created by executing the query
	storeName string
	// The predicate for filtering
	predicate Predicate
	// The aggregaton function to use
	aggregation Aggregation
	// Base query (if exists)
	basequery *Query
}

// Gets the loaded base dataset
func (q *Query) Base() *dataset.DataSet {
	return q.baseDataset
}

// Gets the name of the storing dataset
func (q *Query) BaseName() string {
	if q.baseDataset != nil {
		return q.baseDataset.Name
	}
	return ""
}

// Gets the name of the storing dataset
func (q *Query) StoreName() string {
	return q.storeName
}

// Gets the name of the storing dataset
func (q *Query) FilterPredicate() Predicate {
	return q.predicate
}

// Loads a given dataset (e.g.: FROM x; LOAD x; USE x;)
func (q *Query) Load(dataSet *dataset.DataSet) *Query {
	q.baseDataset = dataSet
	return q
}

// Filters the loaded dataset by a predicate (e.g.: WHERE x; CHOOSE x; FILTER x;)
func (q *Query) Filter(predicate Predicate) *Query {
	q.predicate = predicate
	return q
}

// Stores the query result in a new set (e.g.: INSERT INTO ... ; STORE x;)
func (q *Query) Store(name string) *Query {
	q.storeName = name
	return q
}

// Sets the aggreation of the query
func (q *Query) Aggregate(agg Aggregation) *Query {
	q.aggregation = agg
	return q
}

// Gets the aggregation of the query
func (q *Query) Aggregation() Aggregation {
	return q.aggregation
}

func (q *Query) AggregationIsGrouped() bool {
	if q.aggregation == nil {
		return false
	}
	var _, isgroup = (q.aggregation).(GroupedAggregation)
	return isgroup
}

func (q *Query) BasedOn(query *Query) *Query {
	q.basequery = query
	return q
}

func (q *Query) CreateFrom() Query {
	return Query{
		basequery: q,
	}
}

func (q *Query) GetBaseQuery() *Query {
	return q.basequery
}

func (q Query) MergeQuery() Query {
	if q.basequery == nil {
		return q
	}
	newbase := q.basequery.MergeQuery()

	return Query{
		baseDataset: newbase.baseDataset,
		storeName:   q.storeName,
		predicate: AndPredicate{
			Lhs: newbase.predicate,
			Rhs: q.predicate,
		},
		aggregation: q.aggregation,
	}
}

// Translates the query to a human readable format
func (q *Query) String() string {
	if q == nil {
		return ""
	}
	filterStr := ""
	filter := q.FilterPredicate()
	if filter != nil {
		filterStr = filter.String()
	}
	aggStr := ""
	if q.aggregation != nil {
		aggStr = (q.aggregation).String()
	}
	return fmt.Sprintf("LOAD: %s\nFILTER: %s\nTRANSFORM: %s\nAGGREGATE: %s\nSTORE: %s\n", q.BaseName(), filterStr, "", aggStr, q.StoreName())
}

// Checks if a query only copies a dataset without changing it
func (q Query) IsCopy() bool {
	return q.predicate == nil && q.aggregation == nil
}

// Uses the selectivity estimation to create a new mock dataset based on the query result
func (q *Query) GenerateDataset() dataset.DataSet {
	size := float64(q.baseDataset.GetSize())
	if q.predicate != nil {
		size *= q.predicate.Selectivity(*q.baseDataset)
	}
	return dataset.DataSet{
		Name:          q.StoreName(),
		Count:         nil,
		ExpectedCount: uint64(size),
		Paths:         q.baseDataset.Paths,
		DerivedFrom:   q.baseDataset,
	}
}

// Creates a copy of the query without any aggregation
func (q Query) CopyWithoutAggregation() Query {
	return Query{
		baseDataset: q.baseDataset,
		storeName:   q.storeName,
		predicate:   q.predicate,
		basequery:   q.basequery,
	}
}

/*
	// Transforms the filter result into a new expression, (e.g.: SELECT x; AS x; TRANSFORM x;)
	transform() Query
	// Aggregates the query result  (e.g.: SELECT SUM() ; AGG x;)
	aggregate() Query
*/

func RemoveIntermediateSets(queries []Query) []Query {
	predicates := make(map[string]Predicate)
	baseSets := make(map[string]*dataset.DataSet)
	for i := range queries {
		q := &queries[i]
		basePred, ok := predicates[q.BaseName()]
		if ok {
			// Merge predicate
			q.Filter(AndPredicate{Lhs: basePred, Rhs: q.FilterPredicate()})
			// Set load to parent
			q.Load(baseSets[q.BaseName()])
		}
		predicates[q.StoreName()] = q.FilterPredicate()
		baseSets[q.StoreName()] = q.Base()
		//Reset store
		q.Store("")
	}
	return queries
}
