package dataframeutil

import (
	"database/sql"
	"io/ioutil"
	"os"
	"strings"

	"github.com/kniren/gota/dataframe"
)

var(
	db *sql.DB
)

// Open 데이터베이스 접근 (드라이버네임 string, DB 접속정보 string, maxopenconns int, maxidleconns int)
// ex) dataframeutil.Open("mysql", "id:pw@tcp(127.0.0.1:3306)/TESTDB", 10, 5)
func Open(driverName string, dataSourceName string, MaxOpenConns int, MaxIdleConns int) (*sql.DB, error){
	db, err := sql.Open(driverName, dataSourceName)
	CheckError(err)

	db.SetMaxOpenConns(MaxOpenConns)
	db.SetMaxIdleConns(MaxIdleConns)

	return db, err
}

// Close Open한 DB 닫기
func Close(db *sql.DB){
	db.Close()
}

// Exec sql.Exec와 동일
func Exec(query string, args ...interface{}) (sql.Result, error){
	Result, err := db.Exec(query, args)
	CheckError(err)

	return Result, err
}

// 에러 검사
func CheckError(err error) {
	if err != nil {
		panic(err)
	}
}

// LoadCSV csv 파일을 데이터프레임으로 반환 (파일위치 string, 컬럼 헤더 여부 bool, 구분자 rune) 리턴타입 DataFrame
func LoadCSV(location string, isColHeader bool, delimiter rune) dataframe.DataFrame {
	content, err := ioutil.ReadFile(location)
	CheckError(err)
	ioContent := strings.NewReader(string(content))

	csv := dataframe.ReadCSV(ioContent,
		dataframe.WithDelimiter(delimiter),
		dataframe.HasHeader(isColHeader))

	return csv
}

// SaveCSV 데이터프레임을 csv파일로 저장 (데이터프레임 df, 저장위치 string)
func SaveCSV(df dataframe.DataFrame, location string) {
	f, err := os.Create(location)
	CheckError(err)

	df.WriteCSV(f)
}

// ReplaceElem 데이터프레임의 특정 값들을 원하는 값으로 변경(데이터프레임 df, 이전문자 interface{}, 바꿀문자 interface{}) 리턴타입 DataFrame
// 바꿀 값이 속한 컬럼의 타입과 new의 타입이 동일해야 변경 가능
func ReplaceElem(df dataframe.DataFrame, old interface{}, new interface{}) dataframe.DataFrame {
	for c := 0; c < df.Ncol(); c++ {
		for r := 0; r < df.Nrow(); r++ {
			if df.Elem(r, c).String() == old {
				df.Elem(r, c).Set(new)
			}
		}
	}
	return df
}

// Querytodf mysql DB에 쿼리를 날려 받아온 정보를 데이터프레임으로 반환(데이터베이스 sql.DB, 쿼리 string)
func Querytodf(db *sql.DB, q string) dataframe.DataFrame {
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
