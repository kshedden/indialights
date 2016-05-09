# Directory where all data are located, and all results are placed
DPATH = /nfs/brianmin/work/kshedden/india-10k-new/

# Name of the config file passed to all scripts
CONFIG = config10k.json

# Name of the darkspots vis raw data file (must be a file in DPATH)
DS_VIS_RAW = dark_samp_10k_data.csv.gz

# Name of the village vis raw data file (must be a file in DPATH)
VI_VIS_RAW = latest_good_vis_9315.csv.gz

# Path to the Go installation
export GOROOT = /nfs/brianmin/work/kshedden/go/

# Path to the Go projects directory
export GOPATH = /nfs/brianmin/work/kshedden/fluxrun/


#
# Users should not need to edit below this point
#

GO = $(GOROOT)bin/go

indat = $(DPATH)$(DS_VIS_RAW) $(DPATH)$(VI_VIS_RAW)
match_done = $(DPATH)match_done
reindex_done = $(DPATH)reindex_done
raw_darkspots_done = $(DPATH)raw_darkspots_done
raw_villages_done = $(DPATH)raw_villages_done
background_done = $(DPATH)background_done
subtract_done = $(DPATH)subtract_done
pivot_vis_observed_done = $(DPATH)pivot_vis_observed_done
pivot_background_done = $(DPATH)pivot_background_done
pivot_vis_adjusted_done = $(DPATH)pivot_vis_adjusted_done
pivot_nvalid_done = $(DPATH)pivot_nvalid_done
pivot_bsd_done = $(DPATH)pivot_bsd_done

GOCMD = $(GOPATH)/src/github.com/kshedden/indialights/scripts/

.PHONY: setup all reindex darkspots_raw villages_raw background subtract
.PHONY: pivot_vis_observed pivot_background pivot_vis_adjusted pivot_nvalid pivot_bsd

all: match reindex raw_darkspots raw_villages background subtract\
	pivot_vis_observed pivot_nvalid pivot_bsd pivot_vis_adjusted pivot_background

match: $(match_done)
reindex: $(reindex_done)
raw_darkspots: $(raw_darkspots_done)
raw_villages: $(raw_villages_done)
background: $(background_done)
subtract: $(subtract_done)
pivot_vis_observed: $(pivot_vis_observed_done)
pivot_background: $(pivot_background_done)
pivot_vis_adjusted: $(pivot_vis_adjusted_done)
pivot_nvalid: $(pivot_nvalid_done)
pivot_bsd: $(pivot_bsd_done)

setup:
	$(GO) get -u github.com/kshedden/indialights
	$(GO) get -u github.com/kshedden/ziparray
	$(GO) get -u github.com/paulmach/go.geo

.PHONY: clean_darkspots clean_villages clean

clean_darkspots:
	/bin/rm -rf $(DPATH)darkspots
	/bin/rm -f $(DPATH)raw_darkspots_done
	/bin/rm -f $(DPATH)reindex_darkspots_done

clean_villages:
	/bin/rm -rf $(DPATH)villages
	/bin/rm -f $(DPATH)reindex_darkspots_done
	/bin/rm -f $(DPATH)reindex_villages_done

clean: clean_darkspots clean_villages
	/bin/rm -rf $(DPATH)matches.gob.gz
	/bin/rm -rf $(DPATH)reindex_done

$(match_done): $(indat)
	$(GO) run $(GOCMD)match.go $(CONFIG)

$(reindex_done): $(match_done)
	$(GO) run $(GOCMD)reindex.go $(CONFIG)

$(raw_darkspots_done): $(reindex_done)
	$(GO) run $(GOCMD)raw_to_cols.go $(CONFIG) darkspots
	$(GO) run $(GOCMD)reindex_columns.go $(CONFIG) darkspots

$(raw_villages_done): $(reindex_done)
	$(GO) run $(GOCMD)raw_to_cols.go $(CONFIG) villages
	$(GO) run $(GOCMD)reindex_columns.go $(CONFIG) villages

$(background_done): $(raw_darkspots_done) $(raw_villages_done)
	$(GO) run $(GOCMD)background.go $(CONFIG)

$(subtract_done): $(background_done)
	$(GO) run $(GOCMD)subtract.go $(CONFIG)

$(pivot_vis_observed_done): $(subtract_done)
	$(GO) run $(GOCMD)pivot.go $(CONFIG) vis_observed

$(pivot_background_done): $(subtract_done)
	$(GO) run $(GOCMD)pivot.go $(CONFIG) background

$(pivot_nvalid_done): $(subtract_done)
	$(GO) run $(GOCMD)pivot.go $(CONFIG) nvalid

$(pivot_bsd_done): $(subtract_done)
	$(GO) run $(GOCMD)pivot.go $(CONFIG) bsd

$(pivot_vis_adjusted_done): $(subtract_done)
	$(GO) run $(GOCMD)pivot.go $(CONFIG) vis_adjusted
