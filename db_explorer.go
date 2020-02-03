package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"net/http"
	"net/url"
	"reflect"
	"strings"
)

type config struct {
	db *sql.DB
}

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные
func NewDbExplorer(db *sql.DB) (http.Handler, error) {

	config := config{db: db}

	router := http.NewServeMux()
	router.HandleFunc("/", handler(config))

	return router, nil
}

func handler(c config) func(http.ResponseWriter, *http.Request) {
	return func(writer http.ResponseWriter, request *http.Request) {
		query, ok := url.ParseQuery(request.URL.RawQuery)
		if ok != nil {
			fmt.Println(ok.Error())
		}
		switch request.Method {
		case http.MethodGet:
			{
				path := request.URL.Path
				if len(path) == 1 {
					tables, ok := tableList(c.db, query)
					if ok != nil {
						writer.WriteHeader(http.StatusInternalServerError)
						return
					}
					writer.Write(Response(tables, "tables"))
				} else {
					records, ok := findTableRows(c.db, query, path)
					if ok != nil {
						if sqlError, e := (ok).(*mysql.MySQLError); e && sqlError.Number == 1146 {
							writer.WriteHeader(http.StatusNotFound)
							writer.Write(ResponseError("unknown table"))
						} else {
							writer.WriteHeader(http.StatusInternalServerError)
						}
						return
					}
					responseRecords := ResponseRecords(records, "records")
					writer.Write(responseRecords)
				}
			}
		case http.MethodPost:

		case http.MethodPut:

		case http.MethodDelete:

		default:
			writer.WriteHeader(http.StatusInternalServerError)
		}
	}
}

func tableList(db *sql.DB, query url.Values) ([]string, error) {

	var tables []string
	rows, ok := db.Query("SHOW TABLES;")
	if ok != nil {
		return nil, ok
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		ok := rows.Scan(&name)
		if ok != nil {
			return nil, ok
		}
		tables = append(tables, name)
	}

	return tables, nil
}

func findTableRows(db *sql.DB, query url.Values, path string) ([]map[string]interface{}, error) {

	params := strings.Split(path, "/")
	var objects []map[string]interface{}

	if len(params) > 1 {
		table_name := params[1]
		rows, ok := db.Query(fmt.Sprintf("SELECT * FROM %s", table_name))
		if ok != nil {
			return nil, ok
		}
		defer rows.Close()

		for rows.Next() {
			columnTypes, _ := rows.ColumnTypes()

			values := make([]interface{}, len(columnTypes))
			object := map[string]interface{}{}
			for i, column := range columnTypes {

				v := reflect.New(column.ScanType()).Interface()
				switch v.(type) {
				case *[]uint8:
					v = new(*string)
				case *int32:
					v = new(*int32)
				case *sql.RawBytes:
					v = new(*string)
				default:
					values[i] = v
				}

				object[column.Name()] = v
				values[i] = v
			}

			ok := rows.Scan(values...)
			if ok != nil {
				return nil, ok
			}

			objects = append(objects, object)
		}

	}

	return objects, nil
}

func Response(rows []string, key string) []byte {
	response := make(map[string]interface{})
	responseRows := make(map[string]interface{})
	responseRows[key] = rows
	response["response"] = responseRows
	json, _ := json.Marshal(response)
	return json
}

func ResponseRecords(records []map[string]interface{}, key string) []byte {
	response := make(map[string]interface{})
	responseRows := make(map[string]interface{})
	responseRows[key] = records
	response["response"] = responseRows
	json, _ := json.Marshal(response)
	return json
}

func ResponseError(error string) []byte {
	response := make(map[string]interface{})
	response["error"] = error
	json, _ := json.Marshal(response)
	return json
}
