package main

// reindex maps the village and darkspot ids to consecutive integer
// keys 0, 1, ...
//
// The village id/index associations are written to villages.csv.gz.
// The darkspot id/index associations are written to darkspots.csv.gz.
//
// The village/darkspot matches are written to matches.gob.gz.  The
// matches are stored as an array of arrays, with each nested array
// containing the darkspot integer keys corresponding to one village.
//
// Specifically, match[i] = [j1, j2, ...] means that village i is
// matched to dark spots j1, j2, ... All the i/j values here are
// int64.
//
// Run this script after running match

import (
	"bufio"
	"compress/gzip"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"

	lights "github.com/kshedden/indialights"
)

// map_to_csv writes a string->int map to a csv file
func map_to_csv(mp map[int64]int, fname, title string) {

	keys := make([]int, len(mp))
	i := 0
	for k, _ := range mp {
		keys[i] = int(k)
		i++
	}
	sort.IntSlice(keys).Sort()

	fid, err := os.Create(fname)
	if err != nil {
		panic(err)
	}
	wtr := gzip.NewWriter(fid)
	_, err = wtr.Write([]byte(fmt.Sprintf("id,%s\n", title)))
	if err != nil {
		panic(err)
	}
	for k, _ := range keys {
		v := mp[int64(k)]
		_, err = wtr.Write([]byte(fmt.Sprintf("%d,%d\n", k, v)))
		if err != nil {
			panic(err)
		}
	}
	wtr.Close()
	fid.Close()
}

func main() {

	if len(os.Args) != 2 {
		panic("usage: reindex_matches conf.json")
	}
	conf := lights.GetConf(os.Args[1])

	// File handle for writing the unique village ids
	fname := path.Join(conf.Path, conf.ViIndexFile)
	fid, err := os.Create(fname)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	vi_out := gzip.NewWriter(fid)
	defer vi_out.Close()

	// File handle for writing the unique darkspot ids
	fname = path.Join(conf.Path, conf.DSIndexFile)
	fid, err = os.Create(fname)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	ds_out := gzip.NewWriter(fid)
	defer ds_out.Close()

	// File handle for reading the raw match data
	fname = path.Join(conf.Path, conf.MatchRawFile)
	fid, err = os.Open(fname)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	match_in, err := gzip.NewReader(fid)
	if err != nil {
		panic(err)
	}
	defer match_in.Close()

	// Get the raw file size, so we can write progress messages to the console
	stat, err := fid.Stat()
	if err != nil {
		panic(err)
	}
	fsize := stat.Size()

	// Maps from the original string village and darkspot ids to
	// unique integer ids.
	village_ids := make(map[string]int64)
	darkspot_ids := make(map[string]int64)

	// Array of arrays containing the darkspot ids that are
	// matched to each village
	matches := make([][]int64, 0)

	// Match counts (village to darkspot and darkspot to village)
	match_count_vi := make(map[int64]int)
	match_count_ds := make(map[int64]int)

	// Read the match file
	scanner := bufio.NewScanner(match_in)
	line_count := 0
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimRight(line, "\n")
		fields := strings.Split(line, ",")

		// Progress report
		if line_count%10000000 == 0 {
			pos, err := fid.Seek(0, os.SEEK_CUR)
			if err != nil {
				panic(err)
			}
			fmt.Printf("%7.4f ", float64(pos)/float64(fsize))
		}

		// Look up the village id, create a new id if needed
		var vi_ix, ds_ix int64
		var ok bool
		vi_ix, ok = village_ids[fields[0]]
		if !ok {
			m := int64(len(village_ids))
			village_ids[fields[0]] = m
			vi_ix = m
			_, err = vi_out.Write([]byte(fmt.Sprintf("%d,%s\n", m, fields[0])))
			if err != nil {
				panic(err)
			}
			match_count_vi[vi_ix] = 0
		}

		// Look up the darkspot id, create a new one if needed
		ds_ix, ok = darkspot_ids[fields[1]]
		if !ok {
			m := int64(len(darkspot_ids))
			darkspot_ids[fields[1]] = m
			ds_ix = m
			_, err = ds_out.Write([]byte(fmt.Sprintf("%d,%s\n", m, fields[1])))
			if err != nil {
				panic(err)
			}
			match_count_ds[ds_ix] = 0
		}

		// Update the matches
		if vi_ix >= int64(len(matches)) {
			matches = append(matches, make([]int64, 0, 20))
		}
		matches[vi_ix] = append(matches[vi_ix], ds_ix)
		line_count++

		match_count_vi[vi_ix]++
		match_count_ds[ds_ix]++
	}

	fmt.Printf("\nWriting matches to disk...\n")
	fname = path.Join(conf.Path, conf.MatchGobFile)
	fid, err = os.Create(fname)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	wtr := gzip.NewWriter(fid)
	defer wtr.Close()
	enc := gob.NewEncoder(wtr)
	err = enc.Encode(matches)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Done\n")

	fmt.Printf("Writing match counts to disk...\n")
	fname = path.Join(conf.Path, "village_match_counts.csv.gz")
	map_to_csv(match_count_vi, fname, "darkspots")
	fname = path.Join(conf.Path, "darkspot_match_counts.csv.gz")
	map_to_csv(match_count_ds, fname, "villages")
	fmt.Printf("Done\n")

	fmt.Printf("Writing info to disk...\n")
	info := new(lights.Info)
	info.Nvillage = len(matches)
	info.Nchunk = len(matches) / conf.ChunkSize
	if info.Nchunk*conf.ChunkSize < len(matches) {
		info.Nchunk++
	}
	fname = path.Join(conf.Path, "info.json")
	fid, err = os.Create(fname)
	if err != nil {
		panic(err)
	}
	b, err := json.Marshal(info)
	if err != nil {
		panic(err)
	}
	_, err = fid.Write(b)
	if err != nil {
		panic(err)
	}
	fid.Close()

	// Write empty file to signal completion.
	fname = path.Join(conf.Path, "reindex_done")
	fid, err = os.Create(fname)
	if err != nil {
		panic(err)
	}
	fid.Close()
}
