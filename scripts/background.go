package main

// background computes the background trimmed mean values using the
// darkspots that are matched to each village.  The results are placed
// into files named "background.gz", placed into each date directory.
// These files are gzipped arrays of float64 values, in the same order
// as given in the file "villages.csv.gz".  Two additional diagnostic
// files are also created in each directory: "nvalid.gz" is the sample
// size for each trimmed mean calculation, and "bsd.gz" is the trimmed
// standard deviation, based on the same data used to calculate the
// trimmed mean.
//
// The structure of background is that background[i] = b implies that
// the background vis value for village i is b.
//
// Run background after running reindex_columns

import (
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"log"
	"math"
	"os"
	"path"
	"sort"
	"strings"

	lights "github.com/kshedden/indialights"
	"github.com/kshedden/ziparray"
)

var (
	// The match data (village id to array of darkspot ids)
	match [][]int64

	comm chan *frec

	// Semaphore to control goroutines
	sem chan bool

	// Current number or records processed
	nproc int

	// Flag that the last record has been processed
	all_sent bool

	logger *log.Logger
	conf   lights.Conf
)

// Used to channel goroutine output to file writers
type frec struct {
	path   string
	tmeans []float64
	nvalid []float64
	bsd    []float64
}

// Calculate all statistics for one date
func process(dvec []float64, path string) {

	tmeans := make([]float64, len(match))
	nvalid := make([]float64, len(match))
	bsd := make([]float64, len(match))

	// Reusable workspace
	buf := make([]float64, conf.MaxMatch)

	// Percentile points for trimmed mean
	p1 := conf.MatchLower
	p2 := conf.MatchUpper

	for vi_id, ix := range match {

		// Obtain the valid values in the match set
		ii := 0
		for _, i := range ix {
			if !math.IsNaN(dvec[i]) {
				buf[ii] = dvec[i]
				ii++
			}
		}
		vals := buf[0:ii]

		sort.Float64Slice(vals).Sort()

		// Trimmed mean
		tmean := float64(0)
		n := int(0)
		m := len(vals)
		j1 := int(float64(m) * p1)
		j2 := int(float64(m) * p2)
		for i := j1; i < j2; i++ {
			tmean += vals[i]
			n++
		}
		tmean /= float64(n)

		// Standard deviation (also trimmed)
		sd := float64(0)
		for i := j1; i < j2; i++ {
			u := vals[i] - tmean
			sd += u * u
		}
		sd = math.Sqrt(sd / float64(n))

		tmeans[vi_id] = tmean
		nvalid[vi_id] = float64(n)
		bsd[vi_id] = sd
	}
	rj := &frec{path, tmeans, nvalid, bsd}
	comm <- rj
	<-sem
}

// Loop over the dates, read in the data for each date, and launch a
// goroutine to do the calculations.
func streamdata(dirnames []string) {

	for _, da := range dirnames {

		// Read the darkspot data for one day (note there is
		// only one chunk for darkspot data).
		fname := path.Join(da, "vis_observed_00.gz")
		_, err := os.Stat(fname)
		if os.IsNotExist(err) {
			break
		}
		dvec, err := ziparray.ReadFloat64Array(fname)
		if err != nil {
			logger.Print(err)
			logger.Print(fname)
			continue
		}

		sem <- true
		go process(dvec, da)
		nproc++
	}
	all_sent = true
}

func main() {

	if len(os.Args) != 2 {
		panic(fmt.Sprintf("usage: %s conf.json", os.Args[0]))
	}
	conf = lights.GetConf(os.Args[1])

	// Create a logger
	fname := path.Join(conf.Path, "background.log")
	fid, err := os.Create(fname)
	if err != nil {
		panic(err)
	}
	logger = log.New(fid, "", log.Lshortfile)

	// Get the match mapping
	fname = path.Join(conf.Path, conf.MatchGobFile)
	fid, err = os.Open(fname)
	if err != nil {
		panic(err)
	}
	rdr, err := gzip.NewReader(fid)
	if err != nil {
		panic(err)
	}
	dec := gob.NewDecoder(rdr)
	err = dec.Decode(&match)
	if err != nil {
		panic(err)
	}
	rdr.Close()
	fid.Close()

	basepath := conf.DSBaseDir
	basepath = path.Join(conf.Path, basepath)

	dir_names := lights.GetDirNames(basepath)

	comm = make(chan *frec)

	// Limit number of goroutines
	sem = make(chan bool, 40)

	// Calculate backgrounds in parallel
	go streamdata(dir_names)

	// Write out the backround data as it becomes ready
	iq := 0
	for {
		if all_sent && iq == nproc {
			break
		}

		qr := <-comm

		vpath := strings.Replace(qr.path, conf.DSBaseDir, conf.ViBaseDir, 1)

		// If the path doesn't exist, there is no village vis
		// data and we can skip writing these results.
		_, err := os.Stat(vpath)
		if os.IsNotExist(err) {
			continue
		} else if err != nil {
			logger.Print(err)
			continue
		}

		chunk_idx := -1
		for ii := 0; ii < len(match); ii += conf.ChunkSize {
			chunk_idx += 1
			jj := ii + conf.ChunkSize
			if jj > len(match) {
				jj = len(match)
			}

			// Save means
			fname := path.Join(vpath, fmt.Sprintf("background_%02d.gz", chunk_idx))
			err = ziparray.WriteFloat64Array(qr.tmeans[ii:jj], fname)
			if err != nil {
				logger.Print(err)
				logger.Print(fname)
			}

			// Save valid sample size
			fname = path.Join(vpath, fmt.Sprintf("nvalid_%02d.gz", chunk_idx))
			err = ziparray.WriteFloat64Array(qr.nvalid[ii:jj], fname)
			if err != nil {
				logger.Print(err)
				logger.Print(fname)
			}

			// Save standard deviation
			fname = path.Join(vpath, fmt.Sprintf("bsd_%02d.gz", chunk_idx))
			err = ziparray.WriteFloat64Array(qr.bsd[ii:jj], fname)
			if err != nil {
				logger.Print(err)
				logger.Print(fname)
			}
		}

		if iq%10 == 0 {
			fmt.Printf("%.5f\n", float64(iq)/float64(7280))
		}
		iq++
	}

	fname = path.Join(conf.Path, "done_background")
	fid, err = os.Create(fname)
	if err != nil {
		panic(err)
	}
	fid.Close()
}
