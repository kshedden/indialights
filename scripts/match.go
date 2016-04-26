package main

// match takes lat/lon coordinates for darkspots and village, and
// identifies all the darkspots that lie in a rectangle centered at
// each village.
//
// This is usually the first script to run on a new data set

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/dhconnelly/rtreego"
	lights "github.com/kshedden/indialights"
)

const (
	// Small box at each darkspot
	tol = 0.01

	// Search for matches within this latitude (+/-)
	lat_tol = 2.5

	// Search for matches within this longitude (+/-)
	lon_tol = 2.5
)

var (
	ds_lat []float64
	ds_lon []float64
	vi_lat []float64
	vi_lon []float64
	vi_id  []string

	rt *rtreego.Rtree
)

func get_latlon(fname string, id_ix, lat_ix, lon_ix int) ([]string, []float64, []float64) {

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

	latvec := make([]float64, 0)
	lonvec := make([]float64, 0)
	var idvec []string
	if id_ix != -1 {
		idvec = make([]string, 0)
	}
	scanner := bufio.NewScanner(rdr)
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimRight(line, "\n")
		fields := strings.Split(line, ",")
		lat, err := strconv.ParseFloat(fields[lat_ix], 64)
		if err != nil {
			panic(err)
		}
		lon, err := strconv.ParseFloat(fields[lon_ix], 64)
		if err != nil {
			panic(err)
		}
		latvec = append(latvec, lat)
		lonvec = append(lonvec, lon)
		if id_ix != -1 {
			idvec = append(idvec, fields[id_ix])
		}
	}

	return idvec, latvec, lonvec
}

type DarkSpot struct {
	location rtreego.Point
	idx      string
}

func (s *DarkSpot) Bounds() *rtreego.Rect {
	// define the bounds of s to be a rectangle centered at s.location
	// with side lengths 2 * tol:
	return s.location.ToRect(tol)
}

func main() {

	if len(os.Args) != 2 {
		panic("usage: match conf.json")
	}
	conf := lights.GetConf(os.Args[1])

	// Read the coordinates of darkspots and villages
	fname := path.Join(conf.Path, conf.DSLatLonFile)
	_, ds_lat, ds_lon = get_latlon(fname, -1, 0, 1)
	fname = path.Join(conf.Path, conf.ViInfoFile)
	vi_id, vi_lat, vi_lon = get_latlon(fname, 3, 4, 5)

	// Build a tree of darkspots
	rt = rtreego.NewTree(2, 25, 50)
	for k := 0; k < len(ds_lat); k++ {
		idxs := fmt.Sprintf("%.8f:%.8f", ds_lat[k], ds_lon[k])
		rt.Insert(&DarkSpot{rtreego.Point{ds_lat[k], ds_lon[k]}, idxs})
	}

	// Set up file for writing output
	fname = path.Join(conf.Path, "match_raw.txt.gz")
	out, err := os.Create(fname)
	if err != nil {
		panic(err)
	}
	defer out.Close()
	wtr := gzip.NewWriter(out)
	defer wtr.Close()

	// Query for each village
	for k := 0; k < len(vi_lat); k++ {

		point := rtreego.Point{vi_lat[k] - lat_tol, vi_lon[k] - lon_tol}
		lengths := []float64{2 * lat_tol, 2 * lon_tol}
		bb, _ := rtreego.NewRect(point, lengths)

		matches := rt.SearchIntersect(bb)

		for _, ma := range matches {
			mav := ma.(*DarkSpot)
			line := fmt.Sprintf("%s,%s,%.8f,%.8f\n", vi_id[k], mav.idx, vi_lat[k], vi_lon[k])
			wtr.Write([]byte(line))
		}

		// Progress report
		if k%1000 == 0 {
			fmt.Printf("%.4f\n", float64(k)/float64(len(vi_lat)))
		}
	}
}
