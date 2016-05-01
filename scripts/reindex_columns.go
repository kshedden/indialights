package main

// reindex_columns creates a column of values for each darkspot or
// village, in which the data for the values with id=i is stored in
// position i of the array.  The array is then split into blocks and
// saved in separate files.  The arrays are written to files named
// "vis_observed_##.gz".  After running this script, the files "vis.gz"
// and "id.gz" are no longer needed and can be deleted.
//
// The structure of vis_observed is that vis_observed[i] = v implies
// that village/darkspot i has vis value v.
//
// Run this program after running raw_to_cols

import (
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"sync"

	lights "github.com/kshedden/indialights"
	"github.com/kshedden/ziparray"
)

type mode_type int

const (
	village_mode  = iota
	darkspot_mode = iota
)

var (
	// Semaphore to limit goroutines
	sem chan bool

	conf lights.Conf

	wg sync.WaitGroup
)

func process(dname string, n_rec int) {

	defer wg.Done()

	fname := path.Join(dname, "idvis.gz")
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

	// First fill with NaN
	rv := make([]float64, n_rec)
	for i := 0; i < n_rec; i++ {
		rv[i] = math.NaN()
	}

	// Insert the observed values into their proper positions
	for {
		var id int64
		var vis float64
		err = binary.Read(rdr, binary.LittleEndian, &id)
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		err = binary.Read(rdr, binary.LittleEndian, &vis)
		if err != nil {
			panic(err)
		}
		rv[id] = vis
	}

	// Write out the arrray in chunks
	chunk_idx := 0
	for ii := 0; ii < len(rv); ii += conf.ChunkSize {
		fname = path.Join(dname, fmt.Sprintf("vis_observed_%02d.gz", chunk_idx))
		jj := ii + conf.ChunkSize
		if jj > len(rv) {
			jj = len(rv)
		}
		err = ziparray.WriteFloat64Array(rv[ii:jj], fname)
		if err != nil {
			panic(err)
		}
		chunk_idx += 1
	}

	<-sem
}

func main() {

	if len(os.Args) != 3 {
		panic(fmt.Sprintf("usage: %s conf.json [villages|darkspots]", os.Args[0]))
	}
	conf = lights.GetConf(os.Args[1])

	var indexfname string
	var basepath string
	var mode mode_type
	if os.Args[2] == "villages" {
		indexfname = conf.ViIndexFile
		basepath = conf.ViBaseDir
		mode = village_mode
	} else if os.Args[2] == "darkspots" {
		indexfname = conf.DSIndexFile
		basepath = conf.DSBaseDir
		mode = darkspot_mode
	} else {
		panic(fmt.Sprintf("%s not recognized", os.Args[2]))
	}

	basepath = path.Join(conf.Path, basepath)

	fname := path.Join(conf.Path, indexfname)
	idx := lights.ReadIdx(fname)
	n_rec := len(idx)

	dir_names := lights.GetDirNames(basepath)

	sem = make(chan bool, 10)
	for _, fn := range dir_names {
		sem <- true
		wg.Add(1)
		go process(fn, n_rec)
	}

	wg.Wait()

	// Write empty file to signal completion.
	if mode == village_mode {
		fname = path.Join(conf.Path, "reindex_villages_done")
	} else {
		fname = path.Join(conf.Path, "reindex_darkspots_done")
	}
	fid, err := os.Create(fname)
	if err != nil {
		panic(err)
	}
	fid.Close()
}
