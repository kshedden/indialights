package indialights

// match_stats calculates some summary statistics from the matching
// process.
//
// This is currently incomplete and not part of the main data
// processing pipeline.

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"math"
	"os"
	"path"
	"strconv"
	"strings"

	lights "github.com/kshedden/indialights"
)

func stats() {

	fname := path.Join(conf.Path, conf.MatchRawFile)
	fid, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	rdr, err := gzip.NewReader(fid)
	if err != nil {
		panic(err)
	}

	scanner := bufio.NewScanner(rdr)
	dlat_dist := make([]int64, 52)
	dlon_dist := make([]int64, 52)
	nline := 0
	for scanner.Scan() {

		line := scanner.Text()
		line = strings.TrimRight(line, "\n")
		fields := strings.Split(line, ",")

		vi_lat, err := strconv.ParseFloat(fields[2], 64)
		if err != nil {
			panic(err)
		}
		vi_lon, err := strconv.ParseFloat(fields[3], 64)
		if err != nil {
			panic(err)
		}

		fields1 := strings.Split(fields[1], ":")
		ds_lat, err := strconv.ParseFloat(fields1[0], 64)
		if err != nil {
			panic(err)
		}
		ds_lon, err := strconv.ParseFloat(fields1[1], 64)
		if err != nil {
			panic(err)
		}

		dlat := ds_lat - vi_lat
		dlon := ds_lon - vi_lon
		nline++

		dlat_ix := int((dlat + 2.6) * 10)
		dlon_ix := int((dlon + 2.6) * 10)
		dlat_dist[dlat_ix]++
		dlon_dist[dlon_ix]++

		if nline%10000000 == 0 {
			for k := 0; k < len(dlat_dist); k++ {
				fmt.Printf("%.2f %d\n", (float64(k)-26)/10, dlon_dist[k])
			}
		}

		if math.Abs(dlat) > 2.6 {
			msg := fmt.Sprintf("%v %v %v\n", ds_lat, vi_lat, dlat)
			panic(msg)
		}
		if math.Abs(dlon) > 2.6 {
			msg := fmt.Sprintf("%v %v %v\n", ds_lon, vi_lon, dlon)
			panic(msg)
		}
	}
}

func main() {

	if len(os.Args) != 2 {
		panic(fmt.Sprintf("usage: %s conf.json", os.Args[0]))
	}
	conf = lights.GetConf(os.Args[1])

	stats()
}
