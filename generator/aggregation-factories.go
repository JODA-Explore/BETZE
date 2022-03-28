package generator

import (
	"fmt"
	"math/rand"
	"reflect"
	"strings"

	"github.com/JODA-Explore/BETZE/dataset"
	"github.com/JODA-Explore/BETZE/query"
)

// A AggregationFactory generates a aggregation from a given Datapath
type AggregationFactory interface {
	// Checks whether the aggregation can be used on the given dataset
	IsApplicable(p dataset.DataPath) bool
	// Generates the aggregation
	Generate(p dataset.DataPath, blacklist *Blacklist, ranGen *rand.Rand) query.Aggregation
	// Returns the ID/name of the generated aggregation
	ID() string
	// Returns the Type of the aggregation
	Type() reflect.Type
}

type AggregationFactoryRepo struct {
	allfactories    []AggregationFactory
	chosenfactories []AggregationFactory
}

func GetAggregationFactoryRepo() AggregationFactoryRepo {
	defaultFactories := []AggregationFactory{CountAllAggregationFactory{}, CountAggregationFactory{}, GroupByAggregationFactory{}, SumAggregationFactory{}}
	return AggregationFactoryRepo{
		allfactories: defaultFactories,
	}
}

// Return a list of the chosen aggregations
func (repo AggregationFactoryRepo) GetChosen() []AggregationFactory {
	return repo.chosenfactories
}

// Return a list of all aggregations
func (repo AggregationFactoryRepo) GetAll() []AggregationFactory {
	return repo.allfactories
}

// Return a list of all aggregation IDs
func (repo AggregationFactoryRepo) GetAllIDs() []string {
	ids := []string{}
	for _, pred := range repo.allfactories {
		ids = append(ids, pred.ID())
	}
	return ids
}

// Sets the chosen aggregations to the default aggregations (all)
func (repo *AggregationFactoryRepo) SetDefault() {
	repo.SetAll()
}

// Sets the chosen aggregations to all available aggregations
func (repo *AggregationFactoryRepo) SetAll() {
	repo.chosenfactories = repo.allfactories
}

// Include the Aggregation Factory of the given name
func (repo *AggregationFactoryRepo) Include(id string) error {
	pred := repo.GetByID(id)
	if pred == nil {
		return fmt.Errorf("unknown aggregation with ID '%s'", id)
	}
	repo.chosenfactories = append(repo.chosenfactories, *pred)
	return nil
}

// Excludes a AggregationFactory from the list of chosen factories
func (repo *AggregationFactoryRepo) Exclude(id string) {
	index := 0
	for _, pred := range repo.chosenfactories {
		if !strings.EqualFold(pred.ID(), id) {
			repo.chosenfactories[index] = pred
			index++
		}
	}
	repo.chosenfactories = repo.chosenfactories[:index]
}

// Returns a AggregationFactory by ID
func (repo AggregationFactoryRepo) GetByID(id string) *AggregationFactory {
	id = strings.ToLower(id)
	for _, factory := range repo.allfactories {
		if id == strings.ToLower(factory.ID()) {
			return &factory
		}
	}
	return nil
}

//
// CountAll
//

type CountAllAggregationFactory struct {
}

// Checks wether the aggregation can be used on the given dataset
func (e CountAllAggregationFactory) IsApplicable(p dataset.DataPath) bool {
	return true
}

func (e CountAllAggregationFactory) ID() string {
	return "CountAll"
}

// Generates the aggregation
func (e CountAllAggregationFactory) Generate(p dataset.DataPath, blacklist *Blacklist, ranGen *rand.Rand) query.Aggregation {
	return query.GlobalCountAggregation{}
}

func (e CountAllAggregationFactory) Type() reflect.Type {
	return reflect.TypeOf(query.GlobalCountAggregation{})
}

//
// Count
//

type CountAggregationFactory struct {
}

// Checks wether the aggregation can be used on the given dataset
func (e CountAggregationFactory) IsApplicable(p dataset.DataPath) bool {
	return true
}

func (e CountAggregationFactory) ID() string {
	return "Count"
}

// Generates the aggregation
func (e CountAggregationFactory) Generate(p dataset.DataPath, blacklist *Blacklist, ranGen *rand.Rand) query.Aggregation {
	return query.CountAggregation{Path: p.Path}
}

func (e CountAggregationFactory) Type() reflect.Type {
	return reflect.TypeOf(query.CountAggregation{})
}

//
// GroupBy
//

type GroupByAggregationFactory struct {
}

// Checks wether the aggregation can be used on the given dataset
func (e GroupByAggregationFactory) IsApplicable(p dataset.DataPath) bool {
	return p.HasNumCount() || p.HasStringCount() || p.HasBoolCount()
}

func (e GroupByAggregationFactory) ID() string {
	return "GroupBy"
}

// Generates the aggregation
func (e GroupByAggregationFactory) Generate(p dataset.DataPath, blacklist *Blacklist, ranGen *rand.Rand) query.Aggregation {
	return query.GroupedAggregation{
		Path: p.Path,
		Agg:  query.CountAggregation{Path: p.Path},
	}
}

func (e GroupByAggregationFactory) GenerateWithSubAgg(p dataset.DataPath, blacklist *Blacklist, ranGen *rand.Rand, subAgg query.Aggregation) query.Aggregation {
	return query.GroupedAggregation{
		Path: p.Path,
		Agg:  subAgg,
	}
}

func (e GroupByAggregationFactory) Type() reflect.Type {
	return reflect.TypeOf(query.GroupedAggregation{})
}

//
// Sum
//

type SumAggregationFactory struct {
}

// Checks wether the aggregation can be used on the given dataset
func (e SumAggregationFactory) IsApplicable(p dataset.DataPath) bool {
	return p.HasNumCount()
}

func (e SumAggregationFactory) ID() string {
	return "Sum"
}

// Generates the aggregation
func (e SumAggregationFactory) Generate(p dataset.DataPath, blacklist *Blacklist, ranGen *rand.Rand) query.Aggregation {
	return query.SumAggregation{Path: p.Path}
}

func (e SumAggregationFactory) Type() reflect.Type {
	return reflect.TypeOf(query.SumAggregation{})
}
