package generator

import (
	"github.com/JODA-Explore/BETZE/dataset"
	"github.com/JODA-Explore/BETZE/query"
)

// Generates a aggregation according to the generator specifications
func (g *Generator) generateAggregation(dataset dataset.DataSet) (aggregation query.Aggregation) {
	if g.Aggregations == nil || len(g.Aggregations) == 0 {
		return nil
	}

	random := g.getRand()
	chooser := g.getWeightedPathChooser(dataset)

	valid := false
	for !valid { // Create basic aggregation
		path := chooser.PickSource(random).(string)
		dataPath := dataset.Paths[path]
		aggregation = g.generateAggregationForPath(*dataPath)
		if aggregation != nil { // Check if aggregation is set
			valid = true
		}
	}

	// If groupby enabled
	if g.groupByEnabled() {
		// Try to create GROUP BY
		groupBy := GroupByAggregationFactory{}
		for tries := 0; tries < 3; tries++ {
			path := chooser.PickSource(random).(string)
			dataPath := dataset.Paths[path]
			if dataPath == nil {
				continue
			}
			if groupBy.IsApplicable(*dataPath) {
				aggregation = groupBy.GenerateWithSubAgg(*dataPath, &g.currentBlacklist, g.randomGenerator, aggregation)
				return
			}
		}
	}

	return
}

// Generates a predicate for the given path
func (g *Generator) generateAggregationForPath(path dataset.DataPath) query.Aggregation {
	suitableFactories := []AggregationFactory{}
	groupByID := GroupByAggregationFactory{}.ID()
	for _, factory := range g.Aggregations {
		// Is not Group By and applicable
		if factory.ID() != groupByID && factory.IsApplicable(path) {
			suitableFactories = append(suitableFactories, factory)
		}
	}
	if len(suitableFactories) == 0 {
		return nil
	}
	return suitableFactories[g.randomGenerator.Intn(len(suitableFactories))].Generate(path, &g.currentBlacklist, g.randomGenerator)
}

func (g *Generator) groupByEnabled() bool {
	groupByID := GroupByAggregationFactory{}.ID()
	for _, factory := range g.Aggregations {
		if factory.ID() == groupByID {
			return true
		}
	}
	return false
}
