package engine

import (
	"errors"

	pcommon "github.com/pendulea/pendule-common"
)

type SetType string

const SUPPORTED_BINANCE_PAIR SetType = "SUPPORTED_BINANCE_PAIR"

func GetSetType(s pcommon.SetSettings) (SetType, error) {
	if b, _ := s.IsSupportedBinancePair(); b {
		return SUPPORTED_BINANCE_PAIR, nil
	}
	return "", errors.New("set type not supported")
}
