package main

// pivot converts column-oriented data (one column per date) to
// row-oriented time series files.

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path"
	"sort"
	"strings"
	"sync"

	lights "github.com/kshedden/indialights"
)

var (
	logger        *log.Logger
	dir_names     []string
	base_filename string
	wg            sync.WaitGroup
	conf          lights.Conf
	sem           chan bool
)

func do_chunk(chunk_idx int) {
	defer wg.Done()
	defer func() { <-sem }()

	fname := fmt.Sprintf("%s_%02d.gz", base_filename, chunk_idx)
	fname = path.Join(conf.Path, conf.TSDir, base_filename, fname)
	out, err := os.Create(fname)
	if err != nil {
		logger.Print(fmt.Sprintf("chunk %d\n", chunk_idx))
		logger.Print(err)
		panic(err)
	}
	defer out.Close()
	wtr := gzip.NewWriter(out)
	defer wtr.Close()

	// Load the gzipped data into memory as compressed blobs and
	// create a gzip reader for each date.
	source := make([]*gzip.Reader, len(dir_names))
	for k, date := range dir_names {

		// dat is a compressed binary blob
		fname := path.Join(date, fmt.Sprintf("%s_%02d.gz", base_filename, chunk_idx))
		fid, err := os.Open(fname)
		if err != nil {
			logger.Print(fmt.Sprintf("Missing date: %s\n", date))
			continue
		}
		dat, err := ioutil.ReadAll(fid)
		if err != nil {
			logger.Print(fmt.Sprintf("chunk %d\n", chunk_idx))
			logger.Printf(fmt.Sprintf("date %s\n", date))
			logger.Print(err)
			panic(err)
		}
		fid.Close()

		buf := bytes.NewBuffer(dat)
		source[k], err = gzip.NewReader(buf)
		defer source[k].Close()
	}

	fmt.Printf("Done reading blobs for chunk %d\n", chunk_idx)
	for vix := 0; ; vix++ {
		if vix%1000 == 0 {
			fmt.Printf("Chunk %d, %d\n", chunk_idx, vix)
		}

		for k, date := range dir_names {

			if source[k] == nil {
				err = binary.Write(wtr, binary.LittleEndian, math.NaN())
				if err != nil {
					logger.Print(fmt.Sprintf("village %d within chunk: %d\n", vix, chunk_idx))
					logger.Printf(fmt.Sprintf("date: %s\n", date))
					panic(err)
				}
				continue
			}

			// Read one value from a source and write it to a destination
			var x float64
			err = binary.Read(source[k], binary.LittleEndian, &x)
			if err == io.EOF {
				return
			} else if err != nil {
				logger.Print(fmt.Sprintf("village %d within chunk: %d\n", vix, chunk_idx))
				logger.Printf(fmt.Sprintf("date: %s\n", date))
				panic(err)
			}
			err = binary.Write(wtr, binary.LittleEndian, x)
			if err != nil {
				logger.Print(fmt.Sprintf("village within chunk: %d\n", vix))
				logger.Printf(fmt.Sprintf("date: %s\n", date))
				panic(err)
			}
		}
	}
}

func main() {

	if len(os.Args) != 3 {
		panic(fmt.Sprintf("usage: %s conf.json base_filename", os.Args[0]))
	}
	conf = lights.GetConf(os.Args[1])
	base_filename = os.Args[2]

	// Log errors
	fname := path.Join(conf.Path, "pivot.log")
	fid, err := os.Create(fname)
	if err != nil {
		panic(err)
	}
	logger = log.New(fid, "", log.Lshortfile)

	// Use a semaphore to limit the concurrency
	sem = make(chan bool, 5)

	vi_basepath := conf.ViBaseDir
	vi_basepath = path.Join(conf.Path, vi_basepath)
	dir_names = lights.GetDirNames(vi_basepath)

	// Lexical sort is meaningful for dates
	sort.StringSlice(dir_names).Sort()

	// Create a text file with the dates in the same order that
	// they will appear in the data.
	fname = path.Join(conf.Path, conf.TSDir, base_filename)
	err = os.MkdirAll(fname, 0777)
	fid, err = os.Create(path.Join(fname, "dates.txt.gz"))
	if err != nil {
		panic(err)
	}
	wtr := gzip.NewWriter(fid)
	for _, v := range dir_names {
		sv := strings.Split(v, "/")
		m := len(sv)
		da := sv[m-3] + "-" + sv[m-2] + "-" + sv[m-1]
		_, err = wtr.Write([]byte(da + "\n"))
		if err != nil {
			panic(err)
		}
	}
	wtr.Close()
	fid.Close()

	for chunk_idx := 0; chunk_idx < 30; chunk_idx++ {
		sem <- true
		wg.Add(1)
		go do_chunk(chunk_idx)
	}

	wg.Wait()
}
