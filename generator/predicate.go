package generator

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"

	wr "github.com/mroth/weightedrand"

	"github.com/JODA-Explore/BETZE/dataset"
	"github.com/JODA-Explore/BETZE/query"
)

func (g *Generator) collectPaths(dataset dataset.DataSet) []string {
	i := 0
	keys := make([]string, len(dataset.Paths))
	for k := range dataset.Paths {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	g.randomGenerator.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})
	return keys
}

func collectPredStrings(pred_strs map[string]bool, predicate query.Predicate) error {
	switch v := predicate.(type) {
	case query.AndPredicate:
		collectPredStrings(pred_strs, v.Lhs)
		collectPredStrings(pred_strs, v.Rhs)
	case query.OrPredicate:
		collectPredStrings(pred_strs, v.Lhs)
		collectPredStrings(pred_strs, v.Rhs)
	default:
		str := predicate.String()
		if _, ok := pred_strs[str]; ok {
			return fmt.Errorf("predicate `%s` already contained", str)
		} else {
			pred_strs[str] = true
		}
	}
	return nil
}

func checkAndMergePredStrings(pred_strs map[string]bool, new_pred_strs map[string]bool) error {
	for key := range new_pred_strs {
		if _, ok := pred_strs[key]; ok {
			return fmt.Errorf("predicate `%s` already contained", key)
		}
	}

	for key := range new_pred_strs {
		new_pred_strs[key] = true
	}
	return nil
}

func (g *Generator) getWeightedPathChooser(dataset dataset.DataSet) *wr.Chooser {
	paths := g.collectPaths(dataset)

	// Get a list of the number of "/"" in each path
	path_depth := make(map[string]int)
	for _, path := range paths {
		path_depth[path] = strings.Count(path, "/")
	}

	// Get maximum depth
	max_depth := 0
	for _, depth := range path_depth {
		if depth > max_depth {
			max_depth = depth
		}
	}
	max_depth++

	path_choices := []wr.Choice{}
	for _, path := range paths {
		path_choices = append(path_choices, wr.Choice{
			Weight: uint(math.Pow(2, float64((max_depth-path_depth[path])+1))),
			Item:   path,
		})
	}

	chooser, _ := wr.NewChooser(path_choices...)
	return chooser
}

// Generates a predicate according to the generator specifications
func (g *Generator) generatePredicate(dataset dataset.DataSet) (predicate query.Predicate) {
	selectivity := -1.0

	predicate_strings := make(map[string]bool)

	chain := 0
	for tries := 0; tries < g.MaxTries && (selectivity < g.MinSelectivity || selectivity > g.MaxSelectivity); tries++ {
		var tmpPredicate query.Predicate
		if g.WeightedPaths {
			tmpPredicate = g.generateWeightedRandomPredicate(dataset)
		} else {
			tmpPredicate = g.generateRandomPredicate(dataset)
		}
		if tmpPredicate == nil { // Skip unsuccessfull predicate generations
			continue
		}

		tmp_sel := tmpPredicate.Selectivity(dataset)
		if tmp_sel == 1.0 || tmp_sel == 0 {
			continue
		}

		tmp_strs := make(map[string]bool)
		err := collectPredStrings(tmp_strs, tmpPredicate)
		if err != nil {
			continue
		}
		if predicate == nil || chain > g.MaxChain { // Reset predicate first time, or when maximum chain is reached
			predicate = tmpPredicate
			chain = 1
			predicate_strings = tmp_strs
		} else {
			err = checkAndMergePredStrings(predicate_strings, tmp_strs)
			if err != nil {
				continue
			}
			if selectivity < g.MinSelectivity { // Chain OR if below desired selectivity
				predicate = query.OrPredicate{
					Lhs: predicate,
					Rhs: tmpPredicate,
				}
				chain++
			} else if selectivity > g.MaxSelectivity { // Chain AND if above desired selectivity
				predicate = query.AndPredicate{
					Lhs: predicate,
					Rhs: tmpPredicate,
				}
				chain++
			}
		}
		selectivity = predicate.Selectivity(dataset)
	}
	return
}

// Generates a weightes random predicate
func (g *Generator) generateWeightedRandomPredicate(dataset dataset.DataSet) query.Predicate {
	random := g.getRand()
	chooser := g.getWeightedPathChooser(dataset)

	var predicate query.Predicate
	valid := false
	for !valid {
		path := chooser.PickSource(random).(string)
		dataPath := dataset.Paths[path]
		predicate = g.generatePredicateForPath(*dataPath)
		if predicate != nil { // Check if predicate is set
			valid = true
		}
		if valid { // If predicate was generated, check selectivity
			selectivity := predicate.Selectivity(dataset)
			if selectivity == 0.0 || selectivity == 1.0 {
				valid = false // Set predicate as unvalid. Setting predicate to nil would result in an exception
			}
		}
	}

	return predicate
}

// Generates a truly random predicate
func (g *Generator) generateRandomPredicate(dataset dataset.DataSet) query.Predicate {
	random := g.getRand()
	paths := g.collectPaths(dataset)

	var predicate query.Predicate
	valid := false
	for !valid {
		path := paths[random.Intn(len(paths))]
		dataPath := dataset.Paths[path]
		predicate = g.generatePredicateForPath(*dataPath)
		if predicate != nil { // Check if predicate is set
			valid = true
		}
		if valid { // If predicate was generated, check selectivity
			selectivity := predicate.Selectivity(dataset)
			if selectivity == 0.0 || selectivity == 1.0 {
				valid = false // Set predicate as unvalid. Setting predicate to nil would result in an exception
			}
		}
	}

	return predicate
}

// Generates a predicate for the given path
func (g *Generator) generatePredicateForPath(path dataset.DataPath) query.Predicate {
	suitableFactories := []PredicateFactory{}
	for _, factory := range g.Predicates {
		if factory.IsApplicable(path) {
			suitableFactories = append(suitableFactories, factory)
		}
	}
	if len(suitableFactories) == 0 {
		return nil
	}
	return suitableFactories[g.randomGenerator.Intn(len(suitableFactories))].Generate(path, &g.currentBlacklist, g.randomGenerator)
}

func getRandomKeys(m map[string]float64, randomGenerator *rand.Rand) (s []string) {
	for k := range m {
		s = append(s, k)
	}
	sort.Strings(s)
	randomGenerator.Shuffle(len(s), func(i, j int) {
		s[i], s[j] = s[j], s[i]
	})
	return
}

func randomBool(random_generator *rand.Rand) bool {
	return random_generator.Float32() < 0.5
}
