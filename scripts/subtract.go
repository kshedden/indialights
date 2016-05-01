package main

// subtract walks the villages directory and subtracts the background
// values from each village's vis values.
//
// Run subtract after running background.

import (
	"fmt"
	"log"
	"math"
	"os"
	"path"
	"sync"

	lights "github.com/kshedden/indialights"
	"github.com/kshedden/ziparray"
)

var (
	logger *log.Logger
)

func main() {

	if len(os.Args) != 2 {
		panic(fmt.Sprintf("usage: %s conf.json", os.Args[0]))
	}
	conf := lights.GetConf(os.Args[1])

	fname := path.Join(conf.Path, "info.json")
	info := lights.GetInfo(fname)

	fname = path.Join(conf.Path, "subtract.log")
	fid, err := os.Create(fname)
	if err != nil {
		panic(err)
	}
	logger = log.New(fid, "", log.Lshortfile)

	vi_basepath := conf.ViBaseDir
	vi_basepath = path.Join(conf.Path, vi_basepath)
	dir_names := lights.GetDirNames(vi_basepath)

	var wg sync.WaitGroup
	sem := make(chan bool, 30)

	for _, dir := range dir_names {

		fmt.Printf("%v\n", dir)
		for chunk_idx := 0; chunk_idx < info.Nchunk; chunk_idx++ {

			wg.Add(1)
			sem <- true
			go func(dir string, chunk_idx int) {

				defer wg.Done()
				defer func() { <-sem }()

				fname := path.Join(dir, fmt.Sprintf("background_%02d.gz", chunk_idx))
				bg_data, err := ziparray.ReadFloat64Array(fname)
				if err != nil {
					logger.Print(err)
					logger.Print(dir)
					return
				}

				fname = path.Join(dir, fmt.Sprintf("vis_observed_%02d.gz", chunk_idx))
				vi_data, err := ziparray.ReadFloat64Array(fname)
				if err != nil {
					logger.Print(err)
					logger.Print(dir)
					return
				}

				if len(bg_data) != len(vi_data) {
					logger.Print("mismatched lengths\n")
					logger.Print(dir)
					return
				}

				for i := 0; i < len(vi_data); i++ {
					if !math.IsNaN(vi_data[i]) {
						vi_data[i] -= bg_data[i]
					}
				}

				fname = path.Join(dir, fmt.Sprintf("vis_adjusted_%02d.gz", chunk_idx))
				err = ziparray.WriteFloat64Array(vi_data, fname)
				if err != nil {
					logger.Print(err)
					logger.Print(dir)
				}
			}(dir, chunk_idx)
		}
	}

	wg.Wait()

	// Write empty file to signal completion.
	fname = path.Join(conf.Path, "done_subtract")
	fid, err = os.Create(fname)
	if err != nil {
		panic(err)
	}
	fid.Close()
}
