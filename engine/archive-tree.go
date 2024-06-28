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

	DataFilter func(data string, line []string, header map[string]int) string
	Asset      pcommon.AssetType
}

var BINANCE_SPOT_TRADE_ARCHIVE_TREE = ArchiveDataTree{
	ConsistencyMaxLookbackDays: 2,
	Time: AssetBranch{
		OriginColumnTitle: "time",
		OriginColumnIndex: 4,
	},
	Columns: []AssetBranch{
		{
			OriginColumnTitle: "price",
			OriginColumnIndex: 1,
			Asset:             pcommon.Asset.SPOT_PRICE,
		},
		{
			OriginColumnTitle: "qty",
			OriginColumnIndex: 2,
			DataFilter: func(data string, line []string, header map[string]int) string {
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

var BINANCE_FUTURES_TRADE_ARCHIVE_TREE = ArchiveDataTree{
	ConsistencyMaxLookbackDays: 2,
	Time: AssetBranch{
		OriginColumnTitle: "time",
		OriginColumnIndex: 4,
	},
	Columns: []AssetBranch{
		{
			OriginColumnTitle: "price",
			OriginColumnIndex: 1,
			Asset:             pcommon.Asset.FUTURES_PRICE,
		},
		{
			OriginColumnTitle: "qty",
			OriginColumnIndex: 2,
			DataFilter: func(data string, line []string, header map[string]int) string {
				b, err := strconv.ParseBool(line[5])
				if err != nil {
					log.Fatal(err)
				}
				if !b {
					return "-" + data
				}
				return data
			},
			Asset: pcommon.Asset.FUTURES_VOLUME,
		},
	},
}

var ArchivesIndex = map[ArchiveType]*ArchiveDataTree{
	BINANCE_SPOT_TRADES:    &BINANCE_SPOT_TRADE_ARCHIVE_TREE,
	BINANCE_FUTURES_TRADES: &BINANCE_FUTURES_TRADE_ARCHIVE_TREE,
}
