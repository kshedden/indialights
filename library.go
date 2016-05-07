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

type Conf struct {
	// Path to all files
	Path string

	// Raw dark spot data file
	DSRawFile string

	// Column of dates in raw DS file
	DSDateCol int

	// Column of vis values in raw DS file
	DSVisCol int

	// Column of latitude value in raw DS file
	DSLatCol int

	// Column of longitude value in raw DS file
	DSLonCol int

	// Village data raw file
	ViRawFile string

	// Column of dates in raw village file
	ViDateCol int

	// Column of vis values in raw village file
	ViVisCol int

	// Column of village identifier in raw village file
	ViIdCol int

	// Raw matches data file
	MatchRawFile string

	// Dark spot to village matches
	MatchGobFile string

	// Place to store the dark spot id's in order
	DSIndexFile string

	// Place to store the village id's in order
	ViIndexFile string

	// Geographical information about villages
	ViInfoFile string

	// Raw csv file containing coordinates of dark spots
	DSLatLonFile string

	// Directory for dark spot data
	DSBaseDir string

	// Diretory for village data
	ViBaseDir string

	// Directory for final time series results
	TSDir string

	// Number of villages per chunk
	ChunkSize int

	// Maximum number of darkspots matched to one village
	MaxMatch int

	// Lower quantile point for matching, e.g. 0.25 for 25th percentile
	MatchLower float64

	// Upper quantile point for matching, e.g. 0.75 for 75th percentile
	MatchUpper float64

	// Initial matching identifies stations within +/- LatTol
	// degrees latitude
	LatTol float64

	// Initial matching identifies stations within +/- LonTol
	// degrees longitude
	LonTol float64

	// Darkspot matches must be within this geodesic distance of
	// this geodesic distance of the target village
	MTol float64
}

type Info struct {
	Nvillage int
	Nchunk   int
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

// Read the configuration file
func GetInfo(fname string) Info {

	fid, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	b, err := ioutil.ReadAll(fid)
	if err != nil {
		panic(err)
	}
	var info Info
	err = json.Unmarshal(b, &info)
	if err != nil {
		panic(err)
	}
	return info
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
