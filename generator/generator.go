package generator

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/JODA-Explore/BETZE/dataset"
	"github.com/JODA-Explore/BETZE/languages/joda"
	"github.com/JODA-Explore/BETZE/query"
)

type Generator struct {
	// Seed for the random number generator
	randomGenerator *rand.Rand
	// The minimum selectivity each query should have
	MinSelectivity float64
	// The maximum selectivity each query should have
	MaxSelectivity float64
	// Maximum chained AND/OR predicates
	MaxChain int
	// Maximum tries to roll valid query parts
	MaxTries int
	// Probability to randomly browse to a new node
	RandomBrowseProb float64
	// Probability to go back to previous node
	GoBackProb float64
	// Predicates to use in Generator
	Predicates []PredicateFactory
	// Aggregations to use in Generator
	Aggregations []AggregationFactory
	// Probability to perform an aggregation
	AggregationProb float64
	// # Random jumps
	randomJumps int64
	// # Go back
	goBack int64
	// # Stay
	stay int64
	// Weighted path choosing
	WeightedPaths bool
	// Blacklist
	Blacklists map[string]*Blacklist
	//Current Blacklis
	currentBlacklist Blacklist
	//Network
	network Network
}

type Statistics struct {
	RandomJumps int64
	GoBack      int64
	Stay        int64
}

type Blacklist struct {
	// Ignored Prefixes
	ignoredPrefixes map[string]map[string]struct{}
}

type NetworkEdge struct {
	From  string
	To    string
	Query query.Query
	// 0 = Stay, 1 = Back, 2 = RandomJump, 3 = Query
	JumpType  int
	Timestamp uint
}
type NetworkNode struct {
	DSName    string
	Original  bool
	Size      uint64
	Timestamp uint
}
type Network struct {
	Nodes        map[string]NetworkNode
	Edges        []NetworkEdge
	MaxTimestamp uint
}

func New(seed int64) Generator {
	r := rand.New(rand.NewSource(seed))
	return Generator{
		randomGenerator:  r,
		MaxChain:         3,
		MaxTries:         100,
		MinSelectivity:   0.1,
		MaxSelectivity:   0.9,
		RandomBrowseProb: 0.2,
		GoBackProb:       0.4,
		Blacklists:       make(map[string]*Blacklist),
		network: Network{
			Nodes: make(map[string]NetworkNode),
		},
	}
}

// Returns execution Statistics
func (g *Generator) Statistics() Statistics {
	return Statistics{
		RandomJumps: g.randomJumps,
		GoBack:      g.goBack,
		Stay:        g.stay,
	}
}

// Returns the Network
func (g *Generator) Network() Network {
	return g.network
}

// Returns the config as a string
func (g *Generator) PrintConfig() string {
	ids := []string{}
	for _, pred := range g.Predicates {
		ids = append(ids, pred.ID())
	}
	agg_ids := []string{}
	for _, agg := range g.Aggregations {
		agg_ids = append(agg_ids, agg.ID())
	}
	return fmt.Sprintf("MinSelectivity: %s, MaxSelectivity: %s, MaxChain: %d, MaxTries: %d, RandomBrowseProb: %s, GoBackProb: %s, Weighted-Paths: %t, Predicates: [%s], Aggregations: [%s], AggregationProbability: %s", strconv.FormatFloat(g.MinSelectivity, 'f', -1, 64), strconv.FormatFloat(g.MaxSelectivity, 'f', -1, 64), g.MaxChain, g.MaxTries, strconv.FormatFloat(g.RandomBrowseProb, 'f', -1, 64), strconv.FormatFloat(g.GoBackProb, 'f', -1, 64), g.WeightedPaths, strings.Join(ids, ","), strings.Join(agg_ids, ","), strconv.FormatFloat(g.AggregationProb, 'f', -1, 64))
}

// Returns a random number generator initialized with the seed
func (g *Generator) getRand() *rand.Rand {
	return g.randomGenerator
}

// Returns a full benchmark query set
func (g *Generator) GenerateQuerySet(datasets []dataset.DataSet, num_queries int64) (queries []query.Query) {
	for _, v := range datasets {
		g.network.Nodes[v.Name] = NetworkNode{
			DSName:    v.Name,
			Original:  true,
			Size:      v.GetSize(),
			Timestamp: 0,
		}
	}
	for len(queries) < int(num_queries) {
		var prev_query *query.Query
		if len(queries) > 0 {
			prev_query = &queries[len(queries)-1]
		}
		dataset_ptr, edge := g.chooseDataset(datasets, prev_query)
		if dataset_ptr.GetSize() <= 1 {
			continue
		}
		if dataset_ptr == nil {
			return nil
		}

		g.network.MaxTimestamp++
		edge.Timestamp = g.network.MaxTimestamp
		g.network.Edges = append(g.network.Edges, edge) //Jump Edge
		dataset := *dataset_ptr
		q := g.generateQuery(dataset)
		q.Store(createName(dataset, datasets))
		q.BasedOn(prev_query)
		new_dataset := q.GenerateDataset()
		log.Printf("Created dataset %s (with size %d) from dataset %s (with size %d)", new_dataset.Name, new_dataset.GetSize(), new_dataset.DerivedFrom.Name, new_dataset.DerivedFrom.GetSize())
		datasets = append(datasets, new_dataset)
		queries = append(queries, q)
		g.Blacklists[q.StoreName()] = &g.currentBlacklist
		g.network.MaxTimestamp++
		g.network.Edges = append(g.network.Edges, NetworkEdge{
			From:      q.BaseName(),
			To:        q.StoreName(),
			Query:     q,
			JumpType:  3,
			Timestamp: g.network.MaxTimestamp,
		}) //Query Edge
		g.network.Nodes[q.StoreName()] = NetworkNode{
			DSName:    q.StoreName(),
			Size:      new_dataset.GetSize(),
			Timestamp: g.network.MaxTimestamp,
		}
	}
	log.Printf("Used %d random jumps, %d backtracks, and %d stays", g.randomJumps, g.goBack, g.stay)
	return
}

// Returns a full benchmark query set.
// Each query is tested against the JODA backend
func (g *Generator) GenerateQuerySetWithJoda(datasets []dataset.DataSet, num_queries int64, joda_con joda.JodaConnection) ([]query.Query, error) {
	for _, v := range datasets {
		g.network.Nodes[v.Name] = NetworkNode{
			DSName:    v.Name,
			Original:  true,
			Size:      v.GetSize(),
			Timestamp: 0,
		}
	}
	queries := make([]query.Query, 0)
	for len(queries) < int(num_queries) {
		var prev_query *query.Query
		if len(queries) > 0 {
			prev_query = &queries[len(queries)-1]
		}
		dataset_ptr, edge := g.chooseDataset(datasets, prev_query)
		if dataset_ptr.GetSize() <= 1 {
			continue
		}
		if dataset_ptr == nil {
			return queries, nil
		}
		dataset := *dataset_ptr
		q := g.generateQuery(dataset)
		q.Store(createName(dataset, datasets))
		q.BasedOn(prev_query)

		q_wo_agg := q.CopyWithoutAggregation()
		q_wo_agg = q_wo_agg.MergeQuery()

		q_result, err := joda_con.Query(joda.Joda{}.Translate(q_wo_agg))
		if err != nil {
			return nil, err
		}

		if q_result.Error != "" {
			return nil, fmt.Errorf("could not query JODA: %s", q_result.Error)
		}

		new_size := q_result.Size
		actual_selectivity := float64(new_size) / float64(dataset_ptr.GetSize())

		err = joda_con.RemoveResult(*q_result)
		if err != nil {
			return nil, err
		}

		if new_size == 0 || actual_selectivity < g.MinSelectivity || actual_selectivity > g.MaxSelectivity {
			log.Printf("Actual selectivity not in expected range, discarding query (selectivity %f, calculated %f, desired range [%f,%f])", actual_selectivity, q.FilterPredicate().Selectivity(dataset), g.MinSelectivity, g.MaxSelectivity)
			// Clean up source
			err := joda_con.RemoveSource(q.StoreName())
			if err != nil {
				return nil, err
			}
			continue
		}

		// Analyze
		analyze_time := time.Now()
		new_dataset, err := joda_con.AnalyzeDataset(q.StoreName())
		log.Printf("Analyzed dataset %s in %s (%d ns)", q.StoreName(), time.Since(analyze_time), time.Since(analyze_time).Nanoseconds())
		if err != nil {
			return nil, err
		}
		// Set base set
		new_dataset.DerivedFrom = dataset_ptr

		// Remove source
		err = joda_con.RemoveSource(q.StoreName())
		if err != nil {
			return nil, err
		}

		log.Printf("Created dataset %s (with size %d) from dataset %s (with size %d). Selectivity: %f", new_dataset.Name, new_dataset.GetSize(), new_dataset.DerivedFrom.Name, new_dataset.DerivedFrom.GetSize(), actual_selectivity)

		// Create network
		g.network.MaxTimestamp++
		edge.Timestamp = g.network.MaxTimestamp
		g.network.Edges = append(g.network.Edges, edge) // Jump Edge

		// Add queries/datasets
		datasets = append(datasets, new_dataset)
		queries = append(queries, q)

		g.network.MaxTimestamp++
		g.network.Edges = append(g.network.Edges, NetworkEdge{
			From:      q.BaseName(),
			To:        q.StoreName(),
			Query:     q,
			JumpType:  3,
			Timestamp: g.network.MaxTimestamp,
		}) //Query Edge

		g.network.Nodes[q.StoreName()] = NetworkNode{
			DSName:    q.StoreName(),
			Size:      new_dataset.GetSize(),
			Timestamp: g.network.MaxTimestamp,
		}
		g.Blacklists[q.StoreName()] = &g.currentBlacklist
	}

	return queries, nil
}

// Generates a single query given the dataset
func (g *Generator) generateQuery(dataset dataset.DataSet) (q query.Query) {
	q.Load(&dataset)
	g.currentBlacklist = *g.getBlacklist(dataset.Name)
	predicate := g.generatePredicate(dataset)
	if predicate != nil {
		q.Filter(predicate)
	} else {
		log.Println("Could not generate predicate")
	}

	if g.randomGenerator.Float64() <= g.AggregationProb {
		agg := g.generateAggregation(dataset)
		q.Aggregate(agg)
	}

	if q.IsCopy() {
		log.Println("Error: Could not generate valid query")
	}
	return q
}

func (g *Generator) chooseDataset(datasets []dataset.DataSet, previous_query *query.Query) (*dataset.DataSet, NetworkEdge) {
	if len(datasets) == 0 {
		return nil, NetworkEdge{}
	}
	random := g.getRand()
	prob := random.Float64()

	edge := NetworkEdge{}
	if previous_query != nil {
		edge.From = previous_query.StoreName()
	}
	// If random jump, then jump to random set
	if prob <= g.RandomBrowseProb || previous_query == nil {
		g.randomJumps++
		ds := &datasets[random.Intn(len(datasets))]
		edge.JumpType = 2
		edge.To = ds.Name
		return ds, edge
	} else if prob <= g.RandomBrowseProb+g.GoBackProb { //If go back, then go to previous set
		g.goBack++
		edge.JumpType = 1
		edge.To = previous_query.BaseName()
		return previous_query.Base(), edge
	}

	var ds *dataset.DataSet
	//Get Stay Set
	for i, d := range datasets {
		if d.Name == previous_query.StoreName() {
			ds = &datasets[i]
			break
		}
	}
	g.stay++
	edge.JumpType = 0
	edge.To = ds.Name
	return ds, edge //Else, stay at the lastly generated set
}

// Creates a new name given an previous name
func createName(base_set dataset.DataSet, datasets []dataset.DataSet) (new_name string) {
	unique := false
	for i := 1; !unique; i++ {
		new_name = fmt.Sprintf("%s_%d", base_set.Name, i)
		unique = true
		for _, set := range datasets {
			if set.Name == new_name {
				unique = false
			}
		}
	}
	return
}
