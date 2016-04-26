package main

// Some basic tests, not very comprehensive

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path"
	"strconv"
	"strings"

	lights "github.com/kshedden/indialights"
	"github.com/kshedden/ziparray"
)

var (
	conf lights.Conf
)

// Test the vis_observed_xx.gz chunk files by comparing to the raw text file.
func test1() {

	// Read the list of villages
	fname := path.Join(conf.Path, "villages.csv.gz")
	villages, err := ziparray.ReadStringArray(fname)
	if err != nil {
		panic(err)
	}
	for k, v := range villages {
		u := strings.Split(v, ",")
		villages[k] = u[1]
	}

	// Read some records from the raw file and check them
	fname = path.Join(conf.Path, conf.ViRawFile)
	fid, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	rdr, err := gzip.NewReader(fid)
	if err != nil {
		panic(err)
	}
	scanner := bufio.NewScanner(rdr)
	for k := 0; k < 100; k++ {
		nskip := rand.Int() % 1000
		for j := 0; j < nskip; j++ {
			scanner.Scan()
		}
		line := scanner.Text()
		fields := strings.Split(line, ",")
		date := fields[conf.ViDateCol]
		dates := strings.Split(date, "-")
		year := dates[0]
		month := dates[1]
		day := dates[2]
		vid := fields[conf.ViIdCol]

		vix := -1
		for jj, v := range villages {
			if vid == v {
				vix = jj
				break
			}
		}
		if vix == -1 {
			err = fmt.Errorf("cannot find village %s", vid)
			panic(err)
		}

		bucket := vix / conf.ChunkSize
		posn := vix % conf.ChunkSize

		fname = fmt.Sprintf("vis_observed_%02d.gz", bucket)
		fname = path.Join(conf.Path, conf.ViBaseDir, year, month, day, fname)
		vec, err := ziparray.ReadFloat64Array(fname)
		if err != nil {
			panic(err)
		}

		rvis, err := strconv.ParseFloat(fields[conf.ViVisCol], 64)
		if err != nil {
			panic(err)
		}
		if vec[posn] != rvis {
			panic("mismatch in test1")
		}
	}

	fmt.Printf("test1 passed\n")
}

// Test the time series files against the vis_observed_##.gz chunk files.
func test2() {

	bpath := path.Join(conf.Path, conf.TSDir, "vis_observed")
	fname := path.Join(bpath, "dates.txt.gz")
	dates, err := ziparray.ReadStringArray(fname)
	if err != nil {
		panic(err)
	}
	nd := len(dates)

	for k := 0; k < 10; k++ {

		ida := rand.Int() % len(dates)
		dates := dates[ida]
		datev := strings.Split(dates, "-")
		year := datev[0]
		month := datev[1]
		day := datev[2]

		chunk_idx := rand.Int() % 30

		fname = fmt.Sprintf("vis_observed_%02d.gz", chunk_idx)
		fname = path.Join(conf.Path, conf.ViBaseDir, year, month, day, fname)
		avec, err := ziparray.ReadFloat64Array(fname)
		if err != nil {
			panic(err)
		}

		for j := 0; j < 10; j++ {

			fname = fmt.Sprintf("vis_observed_%02d.gz", chunk_idx)
			fname = path.Join(conf.Path, conf.TSDir, "vis_observed", fname)
			bvec, err := ziparray.ReadFloat64SubArray(fname, j*nd, (j+1)*nd)
			if err != nil {
				panic(err)
			}

			if math.IsNaN(avec[j]) && math.IsNaN(bvec[ida]) {
				continue
			}

			if avec[j] != bvec[ida] {
				fmt.Printf("%v\n", dates)
				fmt.Printf("%v != %v\n\n", avec[j], bvec[ida])
			}
		}
	}

	fmt.Printf("test2 passed\n")
}

func main() {

	if len(os.Args) != 2 {
		panic(fmt.Sprintf("usage: %s conf.json", os.Args[0]))
	}
	conf = lights.GetConf(os.Args[1])

	test1()
	test2()
}
