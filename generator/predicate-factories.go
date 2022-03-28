package generator

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/JODA-Explore/BETZE/dataset"
	"github.com/JODA-Explore/BETZE/query"
	"github.com/adam-lavrik/go-imath/ix"
)

// A PredicateFactory generates a predicate from a given Datapath
type PredicateFactory interface {
	// Checks whether the predicate can be used on the given dataset
	IsApplicable(p dataset.DataPath) bool
	// Generates the predicate
	Generate(p dataset.DataPath, blacklist *Blacklist, ranGen *rand.Rand) query.Predicate
	// Returns the ID/name of the generated predicate
	ID() string
	// Returns the Type of the predicate
	Type() reflect.Type
}

type PredicateFactoryRepo struct {
	allfactories    []PredicateFactory
	chosenfactories []PredicateFactory
}

func GetPredicateFactoryRepo() PredicateFactoryRepo {
	defaultFactories := []PredicateFactory{ExistsPredicateFactory{}, BoolEqualityPredicateFactory{}, IsStringPredicateFactory{}, IntEqualityPredicateFactory{}, FloatComparisonPredicateFactory{}, StrPrefixPredicateFactory{}, ObjectSizePredicateFactory{}, ArraySizePredicateFactory{}}
	return PredicateFactoryRepo{
		allfactories: defaultFactories,
	}
}

// Return a list of the chosen predicates
func (repo PredicateFactoryRepo) GetChosen() []PredicateFactory {
	return repo.chosenfactories
}

// Return a list of all predicates
func (repo PredicateFactoryRepo) GetAll() []PredicateFactory {
	return repo.allfactories
}

// Return a list of all predicate IDs
func (repo PredicateFactoryRepo) GetAllIDs() []string {
	ids := []string{}
	for _, pred := range repo.allfactories {
		ids = append(ids, pred.ID())
	}
	return ids
}

// Sets the chosen predicates to the default predicates (all)
func (repo *PredicateFactoryRepo) SetDefault() {
	repo.SetAll()
}

// Sets the chosen predicates to all available predicates
func (repo *PredicateFactoryRepo) SetAll() {
	repo.chosenfactories = repo.allfactories
}

// Include the Predicate Factory of the given name
func (repo *PredicateFactoryRepo) Include(id string) error {
	pred := repo.GetByID(id)
	if pred == nil {
		return fmt.Errorf("unknown predicate with ID '%s'", id)
	}
	repo.chosenfactories = append(repo.chosenfactories, *pred)
	return nil
}

// Excludes a PredicateFactory from the list of chosen factories
func (repo *PredicateFactoryRepo) Exclude(id string) {
	index := 0
	for _, pred := range repo.chosenfactories {
		if !strings.EqualFold(pred.ID(), id) {
			repo.chosenfactories[index] = pred
			index++
		}
	}
	repo.chosenfactories = repo.chosenfactories[:index]
}

// Returns a PredicateFactory by ID
func (repo PredicateFactoryRepo) GetByID(id string) *PredicateFactory {
	id = strings.ToLower(id)
	for _, factory := range repo.allfactories {
		if id == strings.ToLower(factory.ID()) {
			return &factory
		}
	}
	return nil
}

//
// Exists
//

type ExistsPredicateFactory struct {
}

// Checks wether the predicate can be used on the given dataset
func (e ExistsPredicateFactory) IsApplicable(p dataset.DataPath) bool {
	return true
}

func (e ExistsPredicateFactory) ID() string {
	return "Exists"
}

// Generates the predicate
func (e ExistsPredicateFactory) Generate(p dataset.DataPath, blacklist *Blacklist, ranGen *rand.Rand) query.Predicate {
	return query.ExistsPredicate{Path: p.Path}
}

func (e ExistsPredicateFactory) Type() reflect.Type {
	return reflect.TypeOf(query.ExistsPredicate{})
}

//
// IsString
//
type IsStringPredicateFactory struct {
}

// Checks wether the predicate can be used on the given dataset
func (e IsStringPredicateFactory) IsApplicable(p dataset.DataPath) bool {
	return true
}

func (e IsStringPredicateFactory) ID() string {
	return "IsString"
}

// Generates the predicate
func (e IsStringPredicateFactory) Generate(p dataset.DataPath, blacklist *Blacklist, ranGen *rand.Rand) query.Predicate {
	return query.IsStringPredicate{Path: p.Path}
}

func (e IsStringPredicateFactory) Type() reflect.Type {
	return reflect.TypeOf(query.IsStringPredicate{})
}

//
// IntEquality
//
type IntEqualityPredicateFactory struct {
}

func (factory IntEqualityPredicateFactory) IsApplicable(path dataset.DataPath) bool {

	if path.Inttype != nil && path.Count != nil && *path.Count > 0 && (path.Inttype.Min != nil && path.Inttype.Max != nil) && *path.Inttype.Min != *path.Inttype.Max {
		return true
	}

	return false
}

func (e IntEqualityPredicateFactory) ID() string {
	return "IntEquality"
}

func (e IntEqualityPredicateFactory) Type() reflect.Type {
	return reflect.TypeOf(query.IntEqualityPredicate{})
}

// Generates the predicate
func (e IntEqualityPredicateFactory) Generate(p dataset.DataPath, blacklist *Blacklist, ranGen *rand.Rand) query.Predicate {
	predicate := query.IntEqualityPredicate{
		Path:   p.Path,
		Number: ranGen.Int63n(*p.Inttype.Max-*p.Inttype.Min) + *p.Inttype.Min,
	}
	return predicate
}

//
// FloatComparison
//
type FloatComparisonPredicateFactory struct {
}

func (factory FloatComparisonPredicateFactory) IsApplicable(path dataset.DataPath) bool {
	if path.Floattype != nil && path.Count != nil && *path.Count > 0 && (path.Floattype.Min != nil && path.Floattype.Max != nil) && (*path.Floattype.Min != *path.Floattype.Max) {
		return true
	}

	return false
}

func (e FloatComparisonPredicateFactory) ID() string {
	return "FloatComparison"
}

func (e FloatComparisonPredicateFactory) Type() reflect.Type {
	return reflect.TypeOf(query.FloatComparisonPredicate{})
}

// Generates the predicate
func (e FloatComparisonPredicateFactory) Generate(p dataset.DataPath, blacklist *Blacklist, ranGen *rand.Rand) query.Predicate {
	elt := ((*p.Floattype.Max - *p.Floattype.Min) * ranGen.Float64()) + *p.Floattype.Min
	smaller := randomBool(ranGen)
	predicate := query.FloatComparisonPredicate{
		Path:    p.Path,
		Number:  elt,
		Smaller: smaller,
		Equal:   true,
	}
	return predicate
}

//
// String Equality
//

type StrEqualityPredicateFactory struct {
}

func (factory StrEqualityPredicateFactory) IsApplicable(path dataset.DataPath) bool {
	return false
}

func (e StrEqualityPredicateFactory) ID() string {
	return "StringEquality"
}

func (e StrEqualityPredicateFactory) Type() reflect.Type {
	return reflect.TypeOf(query.StrEqualityPredicate{})
}

// Generates the predicate
func (e StrEqualityPredicateFactory) Generate(p dataset.DataPath, blacklist *Blacklist, ranGen *rand.Rand) query.Predicate {
	return nil
}

//
// String Prefix
//
type StrPrefixPredicateFactory struct {
}

func (factory StrPrefixPredicateFactory) IsApplicable(path dataset.DataPath) bool {
	if path.Stringtype != nil && path.Count != nil && *path.Count > 0 && path.Stringtype.Prefixes != nil && len(path.Stringtype.Prefixes) > 0 {
		return true
	}
	return false
}

func (e StrPrefixPredicateFactory) ID() string {
	return "StrPrefix"
}

func (e StrPrefixPredicateFactory) Type() reflect.Type {
	return reflect.TypeOf(query.StrPrefixPredicate{})
}

// Generates the predicate
func (e StrPrefixPredicateFactory) Generate(p dataset.DataPath, blacklist *Blacklist, ranGen *rand.Rand) query.Predicate {
	// TODO Include min/max desired selectivity for better choosing
	path_type := p.Stringtype
	max_prefixes := 5
	var pred query.Predicate

	// Calculate all prefixes and their probabilities
	all_prefixes := make(map[string]float64)
	one_selectivity := 1.0 / float64(len(path_type.Prefixes))
OUTER:
	for _, prefix := range path_type.Prefixes {
		for length := len(prefix); length >= 1; length-- {
			substr := prefix[:length]
			if utf8.ValidString(substr) {
				if blacklist.prefixBlacklisted(p.Path, substr) {
					continue OUTER
				}
				all_prefixes[substr] = all_prefixes[substr] + one_selectivity
			}
		}
	}

	tmp_prefixes := make([]string, len(path_type.Prefixes))
	copy(tmp_prefixes, path_type.Prefixes)

	epsilon := 0.05
	chosen_selectivity := 0.0
	desired_selectivity := ranGen.Float64()

	//TODO Pre-filter too specific prefixes
	for i := 0; (chosen_selectivity < desired_selectivity-epsilon || chosen_selectivity > desired_selectivity+epsilon) && i < ix.Min(len(all_prefixes), max_prefixes); i++ {
		var tmp_prefix string
		var tmp_selectivity float64
		keys := getRandomKeys(all_prefixes, ranGen)
		for i := range keys {
			prefix := keys[i]
			selectivity := all_prefixes[prefix]
			if chosen_selectivity < desired_selectivity {
				if math.Abs(desired_selectivity-(selectivity+chosen_selectivity)) < math.Abs(desired_selectivity-(tmp_selectivity+chosen_selectivity)) {
					tmp_prefix = prefix
					tmp_selectivity = selectivity
				}
			} else if chosen_selectivity > desired_selectivity {
				if math.Abs(desired_selectivity-(selectivity*chosen_selectivity)) < math.Abs(desired_selectivity-(tmp_selectivity*chosen_selectivity)) {
					tmp_prefix = prefix
					tmp_selectivity = selectivity
				}
			}
		}
		if len(tmp_prefix) == 0 {
			continue
		}
		delete(all_prefixes, tmp_prefix)
		for key := range all_prefixes {
			if strings.HasPrefix(key, tmp_prefix) {
				delete(all_prefixes, key)
			}
		}
		predicate := query.StrPrefixPredicate{
			Path:   p.Path,
			Prefix: tmp_prefix,
		}
		blacklist.blacklistPrefix(p.Path, tmp_prefix)
		if pred == nil {
			pred = predicate
			chosen_selectivity = tmp_selectivity
		} else if chosen_selectivity < desired_selectivity {
			pred = query.OrPredicate{
				Lhs: pred,
				Rhs: predicate,
			}
			chosen_selectivity += tmp_selectivity
		} else if chosen_selectivity > desired_selectivity {
			pred = query.AndPredicate{
				Lhs: pred,
				Rhs: predicate,
			}
			chosen_selectivity *= tmp_selectivity
		}
	}
	return pred
}

//
// Bool Equality
//
type BoolEqualityPredicateFactory struct {
}

func (factory BoolEqualityPredicateFactory) IsApplicable(path dataset.DataPath) bool {

	if path.Booltype != nil && path.Count != nil && *path.Count > 0 && path.Booltype.Count != nil && *path.Booltype.Count > 0 {
		return true
	}

	return false
}

func (e BoolEqualityPredicateFactory) ID() string {
	return "BoolEquality"
}

func (e BoolEqualityPredicateFactory) Type() reflect.Type {
	return reflect.TypeOf(query.BoolEqualityPredicate{})
}

// Generates the predicate
func (e BoolEqualityPredicateFactory) Generate(p dataset.DataPath, blacklist *Blacklist, ranGen *rand.Rand) query.Predicate {
	predicate := query.BoolEqualityPredicate{
		Path:  p.Path,
		Value: randomBool(ranGen),
	}
	return predicate
}

//
// Object Size
//

type ObjectSizePredicateFactory struct {
}

func (factory ObjectSizePredicateFactory) IsApplicable(path dataset.DataPath) bool {
	if path.Objecttype != nil && path.Count != nil && *path.Count > 0 && (path.Objecttype.MinMembers != nil && path.Objecttype.MaxMembers != nil && *path.Objecttype.MaxMembers != 0) {
		return true
	}

	return false
}

func (e ObjectSizePredicateFactory) ID() string {
	return "ObjectSize"
}

func (e ObjectSizePredicateFactory) Type() reflect.Type {
	return reflect.TypeOf(query.ObjectSizeComparisonPredicate{})
}

// Generates the predicate
func (e ObjectSizePredicateFactory) Generate(p dataset.DataPath, blacklist *Blacklist, ranGen *rand.Rand) query.Predicate {
	elt := ((*p.Objecttype.MaxMembers - *p.Objecttype.MinMembers) * ranGen.Uint64()) + *p.Objecttype.MinMembers
	smaller := randomBool(ranGen)
	predicate := query.ObjectSizeComparisonPredicate{
		Path:    p.Path,
		Number:  elt,
		Smaller: smaller,
		Equal:   true,
	}
	return predicate
}

//
// Array Size
//
type ArraySizePredicateFactory struct {
}

func (factory ArraySizePredicateFactory) IsApplicable(path dataset.DataPath) bool {
	if path.Arraytype != nil && path.Count != nil && *path.Count > 0 && (path.Arraytype.MinSize != nil && path.Arraytype.MaxSize != nil && *path.Arraytype.MaxSize != 0) {
		return true
	}

	return false
}

func (e ArraySizePredicateFactory) ID() string {
	return "ArraySize"
}

func (e ArraySizePredicateFactory) Type() reflect.Type {
	return reflect.TypeOf(query.ArraySizeComparisonPredicate{})
}

// Generates the predicate
func (e ArraySizePredicateFactory) Generate(p dataset.DataPath, blacklist *Blacklist, ranGen *rand.Rand) query.Predicate {
	elt := ((*p.Arraytype.MaxSize - *p.Arraytype.MinSize) * ranGen.Uint64()) + *p.Arraytype.MinSize
	smaller := randomBool(ranGen)
	predicate := query.ArraySizeComparisonPredicate{
		Path:    p.Path,
		Number:  elt,
		Smaller: smaller,
		Equal:   true,
	}
	return predicate
}
