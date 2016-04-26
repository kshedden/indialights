package indialights

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

const (
	// Would be good to get rid of this
	Nvillage int = 594980
)

type Conf struct {
	Path         string  // Path to all files
	DSRawFile    string  // Raw dark spot data file
	DSDateCol    int     // Column of dates in raw DS file
	DSVisCol     int     // Column of vis values in raw DS file
	DSLatCol     int     // Column of latitude value in raw DS file
	DSLonCol     int     // Column of longitude value in raw DS file
	ViRawFile    string  // Village data raw file
	ViDateCol    int     // Column of dates in raw village file
	ViVisCol     int     // Column of vis values in raw village file
	ViIdCol      int     // Column of village identifier in raw village file
	MatchRawFile string  // Raw matches data file
	MatchGobFile string  // Dark spot to village matches
	DSIndexFile  string  // Place to store the dark spot id's in order
	ViIndexFile  string  // Place to store the village id's in order
	ViInfoFile   string  // Geographical information about villages
	DSLatLonFile string  // Raw csv file containing coordinates of dark spots
	DSBaseDir    string  // Directory for dark spot data
	ViBaseDir    string  // Diretory for village data
	TSDir        string  // Directory for final time series results
	ChunkSize    int     // Number of villages per chunk
	MaxMatch     int     // Maximum number of darkspots matched to one village
	MatchLower   float64 // Lower quantile point for matching, e.g. 0.25 for 25th percentile
	MatchUpper   float64 // Upper quantile point for matching, e.g. 0.75 for 75th percentile
}

var dir_names_chan chan string

func walk_func(path string, info os.FileInfo, err error) error {
	if strings.HasSuffix(path, "vis.gz") {
		dir := filepath.Dir(path)
		dir_names_chan <- dir
	}
	return nil
}

// GetDirNames returns all subdirectories under a given path.
func GetDirNames(basepath string) []string {

	dir_names_chan = make(chan string)
	go func() {
		filepath.Walk(basepath, walk_func)
		defer close(dir_names_chan)
	}()

	dir_names := make([]string, 0, 100)
	for v := range dir_names_chan {
		dir_names = append(dir_names, v)
	}
	return dir_names
}

// Read the configuration file
func GetConf(fname string) Conf {
	fid, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	b, err := ioutil.ReadAll(fid)
	if err != nil {
		panic(err)
	}
	var conf Conf
	err = json.Unmarshal(b, &conf)
	if err != nil {
		panic(err)
	}
	return conf
}

// ReadIdx reads a map[string]int64 from the given file (written as
// text key,value pairs) and returns it.
func ReadIdx(fname string) map[string]int64 {
	idx := make(map[string]int64)
	fid, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	rdr, err := gzip.NewReader(fid)
	if err != nil {
		panic(err)
	}
	defer rdr.Close()

	scanner := bufio.NewScanner(rdr)
	jj := int64(0)
	for scanner.Scan() {
		line := scanner.Text()
		id := strings.Split(line, ",")[1]
		id = strings.TrimRight(id, "\n")
		idx[id] = jj
		jj++
	}

	return idx
}
