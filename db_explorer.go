package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
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
					result, ok := tableList(c.db, query)
					if ok != nil {
						writer.WriteHeader(http.StatusInternalServerError)
						return
					}
					writer.Write(Response(result, "tables"))
				} else {
					params := strings.Split(path, "/")
					table := params[1]
					id := params[2]
					switch len(params) {
					case 2:
						result, ok := findAllRows(c.db, query, table)
						if ok != nil {
							if sqlError, e := (ok).(*mysql.MySQLError); e && sqlError.Number == 1146 {
								writer.WriteHeader(http.StatusNotFound)
								writer.Write(ResponseError("unknown table"))
							} else {
								writer.WriteHeader(http.StatusInternalServerError)
							}
							return
						}
						responseRecords := ResponseRecords(result, "records")
						writer.Write(responseRecords)
					case 3:
						result, ok := findById(c.db, query, table, id)
						if ok != nil {
							if ok.Error() == "record not found" {
								writer.WriteHeader(http.StatusNotFound)
								writer.Write(ResponseError("record not found"))
							} else {
								writer.WriteHeader(http.StatusInternalServerError)
							}
							return
						}
						responseRecords := ResponseRecord(result, "record")
						writer.Write(responseRecords)
					}

				}
			}
		case http.MethodPost:

			path := request.URL.Path
			params := strings.Split(path, "/")
			table := params[1]
			id := params[2]
			body := request.Body
			result, ok := createUpdateRow(c.db, table, body, id)
			if ok != nil {
				writer.WriteHeader(http.StatusInternalServerError)
				return
			}
			writer.Write(ResponseUpdated(result, "id"))

		case http.MethodPut:
			path := request.URL.Path
			params := strings.Split(path, "/")
			table := params[1]
			id := params[2]
			body := request.Body
			result, ok := createUpdateRow(c.db, table, body, id)
			if ok != nil {
				writer.WriteHeader(http.StatusInternalServerError)
				return
			}
			writer.Write(ResponseCreated(result, "updated"))
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

func findAllRows(db *sql.DB, query url.Values, table string) ([]map[string]interface{}, error) {

	//https://forum.golangbridge.org/t/database-rows-scan-unknown-number-of-columns-json/7378/15
	var objects []map[string]interface{}

	tableName := table

	limit, e := strconv.Atoi(query.Get("limit"))
	if e != nil && limit < 0 {
		return nil, e
	}

	offset, e := strconv.Atoi(query.Get("offset"))
	if e != nil && offset < 0 {
		return nil, e
	}

	rows, ok := db.Query(fmt.Sprintf("SELECT * FROM %s limit ? offset ?", tableName), limit, offset)
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

	return objects, nil
}

func findById(db *sql.DB, query url.Values, table string, id string) (map[string]interface{}, error) {

	var objects map[string]interface{}

	rows, ok := db.Query(fmt.Sprintf("SELECT * FROM %s where %s = ?", table, "id"), id)
	if ok != nil {
		return nil, ok
	}
	defer rows.Close()

	if rows.Next() {
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

		objects = object

		return objects, nil
	}

	return nil, errors.New("record not found")
}

func createUpdateRow(db *sql.DB, table string, body io.ReadCloser, id string) (int, error) {

	rows, ok := ioutil.ReadAll(body)
	if ok != nil {
		return -1, ok
	}
	defer body.Close()

	bodyValues := make(map[string]interface{})
	ok = json.Unmarshal(rows, &bodyValues)
	// POST
	if id == "" {

		var fields, placeholders string
		var values []interface{}
		for k, v := range bodyValues {

			if k == "id" {
				continue
			}

			if len(fields) > 0 {
				fields += ","
				placeholders += ","
			}
			fields += "`" + k + "`"
			values = append(values, v)
			placeholders += "?"
		}

		result, ok := db.Exec(fmt.Sprintf("INSERT INTO %s (%s) VALUES(%s)", table, fields, placeholders), values...)
		if ok != nil {
			return -1, ok
		}

		created, ok := result.LastInsertId()
		if ok != nil {
			return -1, ok
		}

		return int(created), nil

	} else {
		// PUT
		var fields, placeholders string
		var values []interface{}
		for k, v := range bodyValues {

			if k == "id" {
				continue
			}

			if len(fields) > 0 {
				fields += ","
				placeholders += ","
			}
			fields += "`" + k + "`"
			values = append(values, v)
			placeholders += "?"
		}

		result, ok := db.Exec(fmt.Sprintf("INSERT INTO %s (%s) VALUES(%s)", table, fields, placeholders), values...)
		if ok != nil {
			return -1, ok
		}

		updated, ok := result.RowsAffected()
		if ok != nil {
			return -1, ok
		}

		return int(updated), nil
	}

	return 0, nil
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

func ResponseRecord(record map[string]interface{}, key string) []byte {
	response := make(map[string]interface{})
	responseRows := make(map[string]interface{})
	responseRows[key] = record
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

func ResponseCreated(id int, key string) []byte {
	response := make(map[string]interface{})
	responseRows := make(map[string]interface{})
	responseRows[key] = id
	response["response"] = responseRows
	json, _ := json.Marshal(response)
	return json
}

func ResponseUpdated(id int, key string) []byte {
	response := make(map[string]interface{})
	responseRows := make(map[string]interface{})
	responseRows[key] = id
	response["response"] = responseRows
	json, _ := json.Marshal(response)
	return json
}
