package query

import "fmt"

// A Aggregation represents operations performed during the aggregation phase
type Aggregation interface {
	// Translates the aggregation to a human readable format
	String() string
	// The target name of the attribute holding this aggregation
	Name() string
}

/*
* GROUP BY
 */

type GroupedAggregation struct {
	// The path to aggregate
	Path string
	// Subaggregation to use
	Agg Aggregation
}

func (q GroupedAggregation) String() string {
	return fmt.Sprintf("%s GROUP BY '%s'", q.Agg.String(), q.Path)
}

func (q GroupedAggregation) Name() string {
	return q.Agg.Name()
}

/*
* COUNT
 */

type GlobalCountAggregation struct {
	// The path to aggregate
	Path string
}

func (q GlobalCountAggregation) String() string {
	return "COUNT()"
}

func (q GlobalCountAggregation) Name() string {
	return "count"
}

type CountAggregation struct {
	// The path to aggregate
	Path string
}

func (q CountAggregation) String() string {
	return fmt.Sprintf("COUNT('%s')", q.Path)
}

func (q CountAggregation) Name() string {
	return "count"
}

/*
* SUM
 */

type SumAggregation struct {
	// The path to aggregate
	Path string
}

func (q SumAggregation) String() string {
	return fmt.Sprintf("SUM('%s')", q.Path)
}

func (q SumAggregation) Name() string {
	return "sum"
}
