package main

// raw_to_cols places binary raw data files for each darkspot or
// village into a separate directory based on the date.  The id values
// are a stream of binary int64 values and the vis values are a stream
// of binary float64 values.  Both files are gzipped.
//
// This script places two arrays "id.gz" and "vis.gz" into each date
// directory, containing the id and vis values respectively.  These
// values are in arbitrary but corresponding order, e.g. if id = [i1,
// i2, i3, ...] and vis = [v1, v2, v3, ...] then the vis value for
// village/darkspot i1 is v1, etc.
//
// Run this program after running reindex

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"

	lights "github.com/kshedden/indialights"
)

const (
	// Flush each buffer to disk when it becomes this large.
	// There are ~8000 buffers (one for each date), so the total
	// memory used by these buffers is ~8000 * bufsize.
	bufsize int = 800000
)

type mode_type int

const (
	village_mode  = iota
	darkspot_mode = iota
)

func drain_buffers(buffers map[string]*bytes.Buffer, basepath string, final bool) {

	ndrain := 0
	for ky, va := range buffers {

		if !final {
			if va.Len() < bufsize {
				continue
			}
		}

		ndrain++

		// Create the parent directories if needed.
		v := strings.Split(ky, "-")
		dpath := path.Join(basepath, v[0], v[1], v[2])
		err := os.MkdirAll(dpath, 0777)
		if err != nil {
			panic(err)
		}

		dpath_id := path.Join(dpath, "id.gz")
		dpath_vis := path.Join(dpath, "vis.gz")

		// Open or create the id data file
		var fid_id *os.File
		_, err = os.Stat(dpath_id)
		if os.IsNotExist(err) {
			fid_id, err = os.Create(dpath_id)
			if err != nil {
				panic(err)
			}
		} else if err != nil {
			panic(err)
		} else {
			fid_id, err = os.OpenFile(dpath_id, os.O_APPEND|os.O_WRONLY, 0666)
			if err != nil {
				panic(err)
			}
		}

		// Open or create the vis data file
		var fid_vis *os.File
		_, err = os.Stat(dpath_vis)
		if os.IsNotExist(err) {
			fid_vis, err = os.Create(dpath_vis)
			if err != nil {
				panic(err)
			}
		} else if err != nil {
			panic(err)
		} else {
			fid_vis, err = os.OpenFile(dpath_vis, os.O_APPEND|os.O_WRONLY, 0666)
			if err != nil {
				panic(err)
			}
		}
		gid_vis := gzip.NewWriter(fid_vis)
		gid_id := gzip.NewWriter(fid_id)

		svec := va.Bytes()
		jj := 0
		for j := 0; j < len(svec)/16; j++ {
			_, err = gid_id.Write(svec[jj : jj+8])
			if err != nil {
				panic(err)
			}
			_, err = gid_vis.Write(svec[jj+8 : jj+16])
			if err != nil {
				panic(err)
			}
			jj += 16
		}
		va.Reset()
		gid_id.Close()
		gid_vis.Close()
		fid_id.Close()
		fid_vis.Close()
	}
	fmt.Printf("Drained %d buffers\n", ndrain)
}

func main() {

	if len(os.Args) != 3 {
		panic(fmt.Sprintf("usage: %s conf.json [village|darkspot]", os.Args[0]))
	}
	conf := lights.GetConf(os.Args[1])

	var indexfname string
	var rawfname string
	var basepath string
	var date_col, vis_col int
	var lat_col, lon_col int
	mode_string := os.Args[2]
	var mode mode_type
	if mode_string == "village" {
		indexfname = conf.ViIndexFile
		rawfname = conf.ViRawFile
		basepath = conf.ViBaseDir
		date_col = conf.ViDateCol
		vis_col = conf.ViVisCol
		mode = village_mode
	} else if mode_string == "darkspot" {
		indexfname = conf.DSIndexFile
		rawfname = conf.DSRawFile
		basepath = conf.DSBaseDir
		date_col = conf.DSDateCol
		vis_col = conf.DSVisCol
		lat_col = conf.DSLatCol
		lon_col = conf.DSLonCol
		mode = darkspot_mode
	} else {
		panic(fmt.Sprintf("%s not recognized", mode_string))
	}

	basepath = path.Join(conf.Path, basepath)

	_, err := os.Stat(basepath)
	if os.IsNotExist(err) {
		err = os.Mkdir(basepath, 0777)
		if err != nil {
			panic(err)
		}
	} else {
		fmt.Printf("Target directory already exists, this script should only be run on a clean target directory.\n")
		return
	}

	fname := path.Join(conf.Path, rawfname)
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

	fname = path.Join(conf.Path, indexfname)
	idx := lights.ReadIdx(fname)

	// Get file size information so we can write progress reports
	// to the terminal
	stat, _ := fid.Stat()
	fsize := stat.Size()

	buffers := make(map[string]*bytes.Buffer)

	// Loop through the input file
	scanner := bufio.NewScanner(rdr)
	line_count := -1
	for scanner.Scan() {

		line_count++
		line := scanner.Text()
		vals := strings.Split(line, ",")
		da := vals[date_col]

		// Create a buffer for this date if none exists yet
		var buf *bytes.Buffer
		var ok bool
		buf, ok = buffers[da]
		if !ok {
			buf = new(bytes.Buffer)
			buffers[da] = buf
		}

		var idv string
		if mode == village_mode {
			idv = vals[0]
		} else if mode == darkspot_mode {
			// Darkspots are always indexed by coordinates appended like this
			idv = vals[lat_col] + ":" + vals[lon_col]
		} else {
			panic("unrecognized mode")
		}

		if line_count%10000000 == 0 {
			pos, _ := fid.Seek(0, os.SEEK_CUR)
			fmt.Printf("%.5f\n", float64(pos)/float64(fsize))
		}
		if line_count%100000000 == 0 {
			drain_buffers(buffers, basepath, false)
		}

		// If not in the match file, skip it
		id, ok := idx[idv]
		if !ok {
			continue
		}

		// Write the id/vis to the buffer as an 8 byte chunk
		binary.Write(buf, binary.LittleEndian, int64(id))
		vis, err := strconv.ParseFloat(vals[vis_col], 64)
		if err != nil {
			panic(err)
		}
		binary.Write(buf, binary.LittleEndian, vis)
	}

	drain_buffers(buffers, basepath, true)
}
