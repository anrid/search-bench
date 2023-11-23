package data

import (
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/bytedance/sonic"
	"github.com/ikawaha/kagome-dict/ipa"
	"github.com/ikawaha/kagome/v2/tokenizer"
)

var _t *tokenizer.Tokenizer

func KagomeV2Tokenizer() *tokenizer.Tokenizer {
	if _t == nil {
		var err error
		_t, err = tokenizer.New(ipa.Dict(), tokenizer.OmitBosEos())
		if err != nil {
			log.Panic(err)
		}
	}
	return _t
}

func ToJSON(o interface{}) []byte {
	b, err := sonic.Marshal(o)
	if err != nil {
		log.Panic(err)
	}
	return b
}

func ToPrettyJSON(o interface{}) []byte {
	b, err := json.MarshalIndent(o, "", "  ")
	if err != nil {
		log.Panic(err)
	}
	return b
}

func ToUnixTimestamp(s string) int64 {
	t, err := time.Parse("2006-01-02 15:04:05 MST", s)
	if err != nil {
		log.Panic(err)
	}
	return t.UnixMilli()
}

func ToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		log.Panic(err)
	}
	return i
}
