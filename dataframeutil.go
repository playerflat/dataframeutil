package dataframeutil

import (
	"database/sql"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/kniren/gota/dataframe"
)

// 에러 검사
func CheckError(err error) {
	if err != nil {
		panic(err)
	}
}

// LoadCSV csv 파일을 데이터프레임으로 반환 (파일위치 string, 컬럼 헤더 여부 bool, 구분자 rune) 리턴타입 DataFrame
func LoadCSV(location string, isColHeader bool, delimiter rune) dataframe.DataFrame {
	content, _ := ioutil.ReadFile(location)
	ioContent := strings.NewReader(string(content))

	csv := dataframe.ReadCSV(ioContent,
		dataframe.WithDelimiter(delimiter),
		dataframe.HasHeader(isColHeader))

	return csv
}

func SaveCSV(df dataframe.DataFrame, location string) {
	f, err := os.Create("test.csv")
	CheckError(err)

	df.WriteCSV(f)
}

// ReplaceElem 데이터프레임의 특정 값들을 원하는 값으로 변경(데이터프레임 df, 이전문자 string, 바꿀문자 string) 리턴타입 DataFrame
func ReplaceElem(df dataframe.DataFrame, oldstring string, newstring string) dataframe.DataFrame {
	for i := 0; i < df.Nrow(); i++ {
		for j := 0; j < df.Ncol(); j++ {
			if df.Elem(i, j).String() == oldstring {
				df.Elem(i, j).Set(newstring)
			}
		}
	}
	return df
}

// mysql DB에 쿼리를 날려 받아온 정보를 데이터프레임으로 반환(쿼리 string, DB 접속정보 `계정명:비밀번호@연결형식(ip:port)/DB이름`)
func Querytodf(q string, DBinfo string) dataframe.DataFrame {
	// Open database connection
	db, err := sql.Open("mysql", DBinfo)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Execute the query
	rows, err := db.Query(q)
	CheckError(err)

	// Get column names
	columns, err := rows.Columns()
	CheckError(err)

	var template string

	for i := 0; i < len(columns); i++ {
		if i+1 != len(columns) {
			template += columns[i] + ","
		} else {
			template += columns[i] + "\n"
		}
	}

	// Make a slice for the values
	values := make([]sql.RawBytes, len(columns))

	// rows.Scan wants '[]interface{}' as an argument, so we must copy the
	// references into such a slice
	// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Fetch rows
	for rows.Next() {
		// get RawBytes from data
		err = rows.Scan(scanArgs...)
		CheckError(err)

		// Now do something with the data.
		// Here we just print each column as a string.
		var value string
		for i, col := range values {
			// Here we can check if the value is nil (NULL value)
			if col == nil {
				value = "NULL"
			} else {
				value = string(col)
			}
			if i != len(columns)-1 {
				template += value + ","
			} else {
				template += value + "\n"
			}
		}
	}
	err = rows.Err()
	CheckError(err)

	queryResult := dataframe.ReadCSV(strings.NewReader(template))

	return queryResult
}
