package engine

import (
	"log"
	"strconv"

	pcommon "github.com/pendulea/pendule-common"
)

type ArchiveDataTree struct {
	ConsistencyMaxLookbackDays int
	Time                       AssetBranch
	Columns                    []AssetBranch
}

type AssetBranch struct {
	OriginColumnTitle string
	OriginColumnIndex int

	DataFilter func(data string, line []string) string
	Asset      pcommon.AssetType
}

var BINANCE_SPOT_TRADE_ARCHIVE_TREE = ArchiveDataTree{
	ConsistencyMaxLookbackDays: 4,
	Time: AssetBranch{
		OriginColumnTitle: "timestamp",
		OriginColumnIndex: 4,
		DataFilter: func(data string, line []string) string {
			return data
		},
	},
	Columns: []AssetBranch{
		{
			OriginColumnTitle: "price",
			OriginColumnIndex: 1,
			DataFilter:        func(data string, line []string) string { return data },
			Asset:             pcommon.Asset.SPOT_PRICE,
		},
		{
			OriginColumnTitle: "quantity",
			OriginColumnIndex: 2,
			DataFilter: func(data string, line []string) string {
				b, err := strconv.ParseBool(line[5])
				if err != nil {
					log.Fatal(err)
				}
				if !b {
					return "-" + data
				}
				return data
			},
			Asset: pcommon.Asset.SPOT_VOLUME,
		},
	},
}

var ArchivesIndex = map[ArchiveType]*ArchiveDataTree{
	BINANCE_SPOT_TRADES: &BINANCE_SPOT_TRADE_ARCHIVE_TREE,
}
