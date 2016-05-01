PATH = /data/kshedden/Zach_OKeefe/10k/
GO = /data/kshedden/go/bin/go
CONFIG = config10k.json

indat = $(PATH)dark_samp_10k_data.csv.gz $(PATH)latest_good_vis_9315.csv.gz
reindex_done = $(PATH)reindex_done
raw_darkspots_done = $(PATH)raw_darkspots_done
raw_villages_done = $(PATH)raw_villages_done
background_done = $(PATH)background_done
subtract_done = $(PATH)subtract_done

.PHONY: all
all: reindex raw_darkspots raw_villages background subtract\
	pivot_vis_observed pivot_nvalid pivot_bsd pivot_vis_adjusted pivot_background

.PHONY: reindex
reindex: $(reindex_done)

.PHONY: darkspots_raw
raw_darkspots: $(raw_darkspots_done)

.PHONY: villages_raw
raw_villages: $(raw_villages_done)

.PHONY: background
background: $(background_done)

.PHONY: subtract
subtract: $(subtract_done)

.PHONY: pivot_vis_observed
pivot_vis_observed: $(pivot_vis_observed_done)

.PHONY: pivot_background
pivot_background: $(pivot_background_done)

.PHONY: pivot_vis_adjusted
pivot_vis_adjusted: $(pivot_vis_adjusted_done)

.PHONY: pivot_nvalid
pivot_nvalid: $(pivot_nvalid_done)

.PHONY: pivot_bsd
pivot_bsd: $(pivot_bsd_done)


.PHONY: clean_darkspots
clean_darkspots:
	/bin/rm -rf $(PATH)darkspots
	/bin/rm -f $(PATH)raw_darkspots_done
	/bin/rm -f $(PATH)reindex_darkspots_done

.PHONY: clean_villages
clean_villages:
	/bin/rm -rf $(PATH)villages
	/bin/rm -f $(PATH)reindex_darkspots_done
	/bin/rm -f $(PATH)reindex_villages_done

.PHONY: clean
clean: clean_darkspots clean_villages
	/bin/rm -rf $(PATH)matches.gob.gz
	/bin/rm -rf $(PATH)reindex_done

$(reindex_done): $(indat)
	$(GO) run reindex.go $(CONFIG)

$(raw_darkspots_done): $(reindex_done)
	$(GO) run raw_to_cols.go $(CONFIG) darkspots
	$(GO) run reindex_columns.go $(CONFIG) darkspots

$(raw_villages_done): $(reindex_done)
	$(GO) run raw_to_cols.go $(CONFIG) villages
	$(GO) run reindex_columns.go $(CONFIG) villages

$(background_done): $(raw_darkspots_done) $(raw_villages_done)
	$(GO) run background.go $(CONFIG)

$(subtract_done): $(background_done)
	$(GO) run subtract.go $(CONFIG)

$(pivot_vis_observed_done): $(subtract_done)
	$(GO) run pivot.go $(CONFIG) vis_observed

$(pivot_background_done): $(subtract_done)
	$(GO) run pivot.go $(CONFIG) background

$(pivot_nvalid_done): $(subtract_done)
	$(GO) run pivot.go $(CONFIG) nvalid

$(pivot_bsd_done): $(subtract_done)
	$(GO) run pivot.go $(CONFIG) bsd

$(pivot_vis_adjusted_done): $(subtract_done)
	$(GO) run pivot.go $(CONFIG) vis_adjusted
