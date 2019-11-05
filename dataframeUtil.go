package dataframeUtil

import (
	"io/ioutil"
	"strings"

	"github.com/kniren/gota/dataframe"
)

// OpenCSV csv 간단하게 불러오기
func OpenCSV(location string, Header bool, Delimiter rune) dataframe.DataFrame {
	content, _ := ioutil.ReadFile(location)
	ioContent := strings.NewReader(string(content))

	csv := dataframe.ReadCSV(ioContent,
		dataframe.WithDelimiter(Delimiter),
		dataframe.HasHeader(Header))

	return csv
}

// ReplaceNaN 데이터프레임의 NaN값을 원하는 값으로 변경
func ReplaceNaN(df dataframe.DataFrame, NaNwords string, Newwords string) {
	for i := 0; i < df.Nrow(); i++ {
		for j := 0; j < df.Ncol(); j++ {
			if df.Elem(i, j).String() == NaNwords {
				df.Elem(i, j).Set(Newwords)
			}
		}
	}
}
