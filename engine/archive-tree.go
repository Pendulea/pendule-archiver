package engine

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/araddon/dateparse"
	pcommon "github.com/pendulea/pendule-common"
)

var GetBookDepthAssetPercentage = func(asset pcommon.AssetType) (int, error) {
	if asset == pcommon.Asset.BOOK_DEPTH_M1 || asset == pcommon.Asset.BOOK_DEPTH_M2 || asset == pcommon.Asset.BOOK_DEPTH_M3 || asset == pcommon.Asset.BOOK_DEPTH_M4 || asset == pcommon.Asset.BOOK_DEPTH_M5 ||
		asset == pcommon.Asset.BOOK_DEPTH_P1 || asset == pcommon.Asset.BOOK_DEPTH_P2 || asset == pcommon.Asset.BOOK_DEPTH_P3 || asset == pcommon.Asset.BOOK_DEPTH_P4 || asset == pcommon.Asset.BOOK_DEPTH_P5 {
		lastChar := asset[len(asset)-1:]
		percent, err := strconv.Atoi(string(lastChar))
		if err != nil {
			return 0, err
		}

		isPlus := strings.HasPrefix(string(asset), "bd-p")
		isMinus := strings.HasPrefix(string(asset), "bd-m")
		if isPlus {
			return percent, nil
		} else if isMinus {
			return -percent, nil
		}
	}
	return 0, errors.New("invalid asset")
}

func GenericBookDepthDataFilter(asset pcommon.AssetType) func(data string, line []string, header map[string]int) (string, error) {

	return func(data string, line []string, header map[string]int) (string, error) {
		idx, ok := header["percentage"]
		var percent int = 0
		var err error = nil
		if ok {
			percent, err = strconv.Atoi(line[idx])
		} else {
			percent, err = strconv.Atoi(line[1])
		}
		if err != nil {
			return "", err
		}
		if percent < -5 || percent > 5 || percent == 0 {
			return "", errors.New("invalid percentage")
		}
		bdPercent, err := GetBookDepthAssetPercentage(asset)
		if err != nil {
			return "", err
		}
		if percent == bdPercent {
			return data, nil
		}
		return "", nil
	}
}

var GenericTimeDataFilter = func(data string, line []string, header map[string]int) (string, error) {
	t, err := dateparse.ParseAny(data)
	if err != nil {
		return "", err
	}
	utcTime := t.In(time.UTC)
	return pcommon.NewTimeUnitFromTime(utcTime).String(), nil
}

type ArchiveDataTree struct {
	ConsistencyMaxLookbackDays int
	Time                       AssetBranch
	Columns                    []AssetBranch
}

type AssetBranch struct {
	OriginColumnTitle string
	OriginColumnIndex int

	DataFilter func(data string, line []string, header map[string]int) (string, error)
	Asset      pcommon.AssetType
}

var BINANCE_SPOT_TRADE_ARCHIVE_TREE = ArchiveDataTree{
	ConsistencyMaxLookbackDays: 2,
	Time: AssetBranch{
		OriginColumnTitle: "time",
		OriginColumnIndex: 4,
		DataFilter:        GenericTimeDataFilter,
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
			DataFilter: func(data string, line []string, header map[string]int) (string, error) {
				b, err := strconv.ParseBool(line[5])
				if err != nil {
					return "", err
				}
				if !b {
					return "-" + data, nil
				}
				return data, nil
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
		DataFilter:        GenericTimeDataFilter,
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
			DataFilter: func(data string, line []string, header map[string]int) (string, error) {
				b, err := strconv.ParseBool(line[5])
				if err != nil {
					return "", err
				}
				if !b {
					return "-" + data, nil
				}
				return data, nil
			},
			Asset: pcommon.Asset.FUTURES_VOLUME,
		},
	},
}

var BINANCE_BOOK_DEPTH_ARCHIVE_TREE = ArchiveDataTree{
	ConsistencyMaxLookbackDays: 3,
	Time: AssetBranch{
		OriginColumnTitle: "timestamp",
		OriginColumnIndex: 0,
		DataFilter:        GenericTimeDataFilter,
	},
	Columns: []AssetBranch{
		{
			OriginColumnTitle: "depth",
			OriginColumnIndex: 2,
			Asset:             pcommon.Asset.BOOK_DEPTH_M1,
			DataFilter:        GenericBookDepthDataFilter(pcommon.Asset.BOOK_DEPTH_M1),
		},
		{
			OriginColumnTitle: "depth",
			OriginColumnIndex: 2,
			Asset:             pcommon.Asset.BOOK_DEPTH_M2,
			DataFilter:        GenericBookDepthDataFilter(pcommon.Asset.BOOK_DEPTH_M2),
		},
		{
			OriginColumnTitle: "depth",
			OriginColumnIndex: 2,
			Asset:             pcommon.Asset.BOOK_DEPTH_M3,
			DataFilter:        GenericBookDepthDataFilter(pcommon.Asset.BOOK_DEPTH_M3),
		},
		{
			OriginColumnTitle: "depth",
			OriginColumnIndex: 2,
			Asset:             pcommon.Asset.BOOK_DEPTH_M4,
			DataFilter:        GenericBookDepthDataFilter(pcommon.Asset.BOOK_DEPTH_M4),
		},
		{
			OriginColumnTitle: "depth",
			OriginColumnIndex: 2,
			Asset:             pcommon.Asset.BOOK_DEPTH_M5,
			DataFilter:        GenericBookDepthDataFilter(pcommon.Asset.BOOK_DEPTH_M5),
		},
		{
			OriginColumnTitle: "depth",
			OriginColumnIndex: 2,
			Asset:             pcommon.Asset.BOOK_DEPTH_P1,
			DataFilter:        GenericBookDepthDataFilter(pcommon.Asset.BOOK_DEPTH_P1),
		},
		{
			OriginColumnTitle: "depth",
			OriginColumnIndex: 2,
			Asset:             pcommon.Asset.BOOK_DEPTH_P2,
			DataFilter:        GenericBookDepthDataFilter(pcommon.Asset.BOOK_DEPTH_P2),
		},
		{
			OriginColumnTitle: "depth",
			OriginColumnIndex: 2,
			Asset:             pcommon.Asset.BOOK_DEPTH_P3,
			DataFilter:        GenericBookDepthDataFilter(pcommon.Asset.BOOK_DEPTH_P3),
		},
		{
			OriginColumnTitle: "depth",
			OriginColumnIndex: 2,
			Asset:             pcommon.Asset.BOOK_DEPTH_P4,
			DataFilter:        GenericBookDepthDataFilter(pcommon.Asset.BOOK_DEPTH_P4),
		},
		{
			OriginColumnTitle: "depth",
			OriginColumnIndex: 2,
			Asset:             pcommon.Asset.BOOK_DEPTH_P5,
			DataFilter:        GenericBookDepthDataFilter(pcommon.Asset.BOOK_DEPTH_P5),
		},
	},
}

var BINANCE_FUTURES_METRICS_ARCHIVE_TREE = ArchiveDataTree{
	ConsistencyMaxLookbackDays: 3,
	Time: AssetBranch{
		OriginColumnTitle: "create_time",
		OriginColumnIndex: 0,
		DataFilter:        GenericTimeDataFilter,
	},
	Columns: []AssetBranch{
		{
			OriginColumnTitle: "sum_open_interest",
			OriginColumnIndex: 2,
			Asset:             pcommon.Asset.METRIC_SUM_OPEN_INTEREST,
		},
		{
			OriginColumnTitle: "count_toptrader_long_short_ratio",
			OriginColumnIndex: 4,
			Asset:             pcommon.Asset.METRIC_COUNT_TOP_TRADER_LONG_SHORT_RATIO,
		},
		{
			OriginColumnTitle: "sum_toptrader_long_short_ratio",
			OriginColumnIndex: 5,
			Asset:             pcommon.Asset.METRIC_SUM_TOP_TRADER_LONG_SHORT_RATIO,
		},
		{
			OriginColumnTitle: "count_long_short_ratio",
			OriginColumnIndex: 6,
			Asset:             pcommon.Asset.METRIC_COUNT_LONG_SHORT_RATIO,
		},
		{
			OriginColumnTitle: "sum_taker_long_short_vol_ratio",
			OriginColumnIndex: 7,
			Asset:             pcommon.Asset.METRIC_SUM_TAKER_LONG_SHORT_VOL_RATIO,
		},
	},
}

var ArchivesIndex = map[ArchiveType]*ArchiveDataTree{
	BINANCE_SPOT_TRADES:    &BINANCE_SPOT_TRADE_ARCHIVE_TREE,
	BINANCE_FUTURES_TRADES: &BINANCE_FUTURES_TRADE_ARCHIVE_TREE,
	BINANCE_BOOK_DEPTH:     &BINANCE_BOOK_DEPTH_ARCHIVE_TREE,
	BINANCE_METRICS:        &BINANCE_FUTURES_METRICS_ARCHIVE_TREE,
}
