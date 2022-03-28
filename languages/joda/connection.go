package joda

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"strings"

	"github.com/JODA-Explore/BETZE/dataset"
	"github.com/adam-lavrik/go-imath/i64"
)

// A connection to a JODA server.
// Can be used to interact with and query the JODA instance.
type JodaConnection struct {
	host string
}

// Returns system information of the JODA server
func (con *JodaConnection) System() (*System, error) {
	url := con.getUrl("/system")

	resp, reqErr := http.Get(url)
	if reqErr != nil {
		return nil, reqErr
	}
	if resp.Body != nil {
		defer resp.Body.Close()
	}
	body, readErr := ioutil.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	system := System{}
	jsonErr := json.Unmarshal(body, &system)
	if jsonErr != nil {
		return nil, jsonErr
	}

	return &system, nil
}

// Returns all stored sources of the server
func (con *JodaConnection) Sources() (*Sources, error) {
	url := con.getUrl("/sources")

	resp, reqErr := http.Get(url)
	if reqErr != nil {
		return nil, reqErr
	}
	if resp.Body != nil {
		defer resp.Body.Close()
	}
	body, readErr := ioutil.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	sources := Sources{}
	jsonErr := json.Unmarshal(body, &sources)
	if jsonErr != nil {
		return nil, jsonErr
	}

	return &sources, nil
}

// Queries the system and returns the answer.
// The actual result set of the query has to be extracted with the "Result function"
func (con *JodaConnection) Query(query string) (*Query, error) {
	req_url := con.getUrl("/query")

	resp, reqErr := http.PostForm(req_url, url.Values{"query": {query}})
	if reqErr != nil {
		return nil, reqErr
	}
	if resp.Body != nil {
		defer resp.Body.Close()
	}
	body, readErr := ioutil.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	query_response := Query{}
	jsonErr := json.Unmarshal(body, &query_response)
	if jsonErr != nil {
		return nil, jsonErr
	}

	return &query_response, nil
}

// Retrieves the result of a query
func (con *JodaConnection) Result(result_id int) (*Result, error) {
	url := con.getUrl(fmt.Sprintf("/result?id=%d", result_id))

	resp, reqErr := http.Get(url)
	if reqErr != nil {
		return nil, reqErr
	}
	if resp.Body != nil {
		defer resp.Body.Close()
	}
	body, readErr := ioutil.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	result := Result{}
	jsonErr := json.Unmarshal(body, &result)
	if jsonErr != nil {
		return nil, jsonErr
	}

	return &result, nil
}

// Handles a Query return and retrieves the result if the query was successful.
// The result is then removed from the backend
func (con *JodaConnection) HandleResult(query_result Query) (*Result, error) {
	if query_result.Error != "" {
		return nil, fmt.Errorf("could not query JODA: %s", query_result.Error)
	}

	// Empty result set
	if query_result.Success == 2 {
		return &Result{}, nil
	}

	// Non valid result
	if query_result.Success < 3 {
		return nil, fmt.Errorf("JODA result error code: %d", query_result.Success)
	}

	result_set, result_err := con.Result(query_result.Success)
	if result_err != nil {
		return nil, result_err
	}

	remove_err := con.RemoveResult(query_result)
	if remove_err != nil {
		return nil, remove_err
	}

	return result_set, nil
}

// Removes the result from the system
func (con *JodaConnection) RemoveResult(result Query) error {
	url := con.getUrl(fmt.Sprintf("/delete?result=%d", result.Success))

	_, reqErr := http.Get(url)
	if reqErr != nil {
		return reqErr
	}
	return nil
}

// Removes a source from the system
func (con *JodaConnection) RemoveSource(name string) error {
	url := con.getUrl(fmt.Sprintf("/delete?name=%s", name))

	_, reqErr := http.Get(url)
	if reqErr != nil {
		return reqErr
	}
	return nil
}

// Analyzes a source and returns the result
func (con *JodaConnection) AnalyzeSource(name string) (*Analyze, error) {
	query := fmt.Sprintf("LOAD %s AGG ('':ATTSTAT(''))", name)

	query_result, query_err := con.Query(query)
	if query_err != nil {
		return nil, query_err
	}

	result_set, result_err := con.HandleResult(*query_result)
	if result_err != nil {
		return nil, result_err
	}
	// Analyzation of empty set
	if len(result_set.Result) != 1 {
		return &Analyze{}, nil
	}

	byteData, _ := json.Marshal(result_set.Result[0])
	analyze := Analyze{}
	json_err := json.Unmarshal(byteData, &analyze)
	if json_err != nil {
		return nil, json_err
	}

	return &analyze, nil
}

// Builds an URL String for the given endpoint.
// endpoint should have a prefixed slash
func (con *JodaConnection) getUrl(endpoint string) string {
	return fmt.Sprintf("%s%s%s", con.host, api_prefix, endpoint)
}

func (con *JodaConnection) AnalyzeDataset(source string) (dataset.DataSet, error) {
	analyze, analyze_err := con.AnalyzeSource(source)
	if analyze_err != nil {
		return dataset.DataSet{}, analyze_err
	}

	var paths = map[string]*dataset.DataPath{}

	analyze_to_paths(analyze, &paths, "")
	count := analyze.CountTotal
	ds := dataset.DataSet{
		Name:          source,
		Count:         &count,
		ExpectedCount: count,
		Paths:         paths,
		DerivedFrom:   nil,
	}

	err := con.analyze_strings(&ds, 10)

	if err != nil {
		return dataset.DataSet{}, err
	}

	return ds, nil
}

func (con *JodaConnection) analyze_strings(dataset *dataset.DataSet, max_prefix_length uint) error {
	max_distinct_prefixes := 1000
	var paths []string
	// Collect all string paths
	for _, path := range dataset.Paths {
		if path.Stringtype != nil && path.Stringtype.Count != nil && *path.Stringtype.Count > 0 {
			paths = append(paths, path.Path)
		}
	}

	// Increase prefix length until enough prefixes exist, up until max_prefix_length
	for prefix_length := 1; prefix_length <= int(max_prefix_length); prefix_length++ {
		if len(paths) == 0 { // Do not attempt to analyze strings if there aren't any
			return nil
		}

		var agg_predicates []string
		for _, path := range paths {
			agg_predicates = append(agg_predicates, fmt.Sprintf("('/%s': DISTINCT(SUBSTR('%s',0,%d))) ", strings.ReplaceAll(path, "/", "~1"), path, prefix_length))
		}

		query := fmt.Sprintf("LOAD %s AGG %s", dataset.Name, strings.Join(agg_predicates, ","))
		query_resp, err := con.Query(query)

		if err != nil {
			return err
		}

		res, err := con.HandleResult(*query_resp)

		if err != nil {
			return err
		}

		if len(res.Result) != 1 {
			return fmt.Errorf("expected one result document, got %d", len(res.Result))
		}

		path_map, ok := res.Result[0].(map[string]interface{})
		if !ok {
			return fmt.Errorf("query result has unrecognized format")
		}

		for path, prefixes := range path_map {
			path = strings.ReplaceAll(path, "~1", "/")
			prefixes, ok := prefixes.([]interface{})
			if !ok {
				return fmt.Errorf("query result entry has unrecognized format")
			}
			var prefix_list []string

			for _, prefix := range prefixes {
				prefix, ok := prefix.(string)
				if !ok {
					return fmt.Errorf("prefix has unrecognized type")
				}
				prefix_list = append(prefix_list, prefix)
			}

			if len(prefix_list) > max_distinct_prefixes || len(dataset.Paths[path].Stringtype.Prefixes) == len(prefix_list) { // Remove paths with too many prefixes or if all prefixes are already known
				// Remove Path
				paths = remove(paths, path)
			}

			dataset.Paths[path].Stringtype.Prefixes = prefix_list
		}
	}

	return nil
}

func remove(s []string, r string) []string {
	for i, v := range s {
		if v == r {
			return append(s[:i], s[i+1:]...)
		}
	}
	return s
}

func analyze_to_paths(analyze *Analyze, paths *map[string]*dataset.DataPath, current_path string) {
	// Copy values to prevent pointer dying

	// Counts
	str_count := uint64(analyze.CountString)
	float_count := uint64(analyze.CountFloat)
	int_count := uint64(analyze.CountInt)
	num_count := uint64(analyze.CountNumber)
	bool_count := uint64(analyze.CountBoolean)
	false_count := uint64(analyze.CountFalse)
	true_count := uint64(analyze.CountTrue)
	null_count := uint64(analyze.CountNull)
	count := uint64(analyze.CountTotal)
	object_count := uint64(analyze.CountObject)
	array_count := uint64(analyze.CountArray)

	// Min/Max

	// Types
	float_type := dataset.FloatType{
		Count:  &num_count,
		Min:    nil,
		Max:    nil,
		Unique: nil,
	}
	int_type := dataset.IntType{
		Count:  &int_count,
		Min:    nil,
		Max:    nil,
		Unique: nil,
	}
	if float_count > 0 || int_count > 0 {
		min := math.MaxFloat64
		max := -math.MaxFloat64
		float_type.Min = &min
		float_type.Max = &max
	}
	if float_count > 0 {
		min_float := math.Min(*float_type.Min, float64(*analyze.MinFloat))
		max_float := math.Max(*float_type.Max, float64(*analyze.MaxFloat))
		float_type.Min = &min_float
		float_type.Max = &max_float
	}
	if int_count > 0 {
		min_float := math.Min(*float_type.Min, float64(*analyze.MinInt))
		max_float := math.Max(*float_type.Max, float64(*analyze.MaxInt))
		float_type.Min = &min_float
		float_type.Max = &max_float
		min_int := i64.Min(math.MaxInt64, *analyze.MinInt)
		max_int := i64.Max(math.MinInt64, *analyze.MaxInt)
		int_type.Min = &min_int
		int_type.Max = &max_int
	}
	obj_Type := dataset.ObjectType{
		Count:      &object_count,
		MinMembers: nil,
		MaxMembers: nil,
	}
	if object_count > 0 {
		min := *analyze.MinMember
		max := *analyze.MinMember
		obj_Type.MinMembers = &min
		obj_Type.MaxMembers = &max
	}

	arr_Type := dataset.ArrayType{
		Count:   &array_count,
		MinSize: nil,
		MaxSize: nil,
	}
	if array_count > 0 {
		min := *analyze.MinSize
		max := *analyze.MinSize
		arr_Type.MinSize = &min
		arr_Type.MaxSize = &max
	}

	(*paths)[current_path] = &dataset.DataPath{
		Path:       current_path,
		Stringtype: &dataset.StringType{Count: &str_count, Min: nil, Max: nil, Unique: nil},
		Floattype:  &float_type,
		Inttype:    &int_type,
		Booltype:   &dataset.BooleanType{Count: &bool_count, FalseCount: &false_count, TrueCount: &true_count},
		Nulltype:   &dataset.NullType{Count: &null_count},
		Objecttype: &obj_Type,
		Arraytype:  &arr_Type,
		Count:      &count,
	}

	for _, child := range analyze.Children {
		analyze_to_paths(&child, paths, fmt.Sprintf("%s/%s", current_path, child.Key))
	}

}

func (con *JodaConnection) GetDatasets(sets []string) ([]dataset.DataSet, error) {
	if con == nil {
		return nil, errors.New("JODA provider requires JODA support")
	}

	sources, source_err := con.Sources()
	if source_err != nil {
		return nil, source_err
	}

	var datasets []dataset.DataSet
	for _, source := range *sources {
		var skip = true
		if len(sets) > 0 {
			for _, source_name := range sets {
				if strings.EqualFold(source.Name, source_name) {
					skip = false
				}
			}
		} else {
			skip = false
		}
		if skip {
			continue
		}

		dataset, err := con.AnalyzeDataset(source.Name)
		if err != nil {
			return nil, err
		}

		datasets = append(datasets, dataset)
	}

	return datasets, nil
}
