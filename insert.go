package pg_util

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

var (
	insertCache  sync.Map
	dedupMapPool = sync.Pool{
		New: func() interface{} {
			return make(map[string]struct{})
		},
	}
)

// Options for building insert statement
type InsertOpts struct {
	// Table to insert into
	Table string

	// Struct that will have all its public fields written to the database.
	//
	// Use `db:"name"` to override the default name of a column.
	//
	// Tags with ",string" after the name will be converted to a string before
	// being passed to the driver. This is useful in some cases like encoding to
	// Postgres domains. This also works, if the name part of the tag is empty.
	// Examples: `db:"name,string"` `db:",string"`
	//
	// Fields with a `db:"-"` tag will be skipped
	//
	// First the fields in struct itself are scanned and then the fields in any
	// embedded structs using depth first search.
	// If duplicate column names (from the struct field name or `db` struct tag)
	// exist, the first found value will ber used.
	Data interface{}

	// Optional prefix to statement
	Prefix string

	// Optional suffix to statement
	Suffix string
}

// Build and cache insert statement for all fields of data. This includes
// embedded struct fields.
//
// See InsertOpts for further documentation.
func BuildInsert(o InsertOpts) (sql string, args []interface{}) {
	rootT := reflect.TypeOf(o.Data)
	k := struct {
		table, prefix, suffix string
		typ                   reflect.Type
	}{
		table:  o.Table,
		prefix: o.Prefix,
		suffix: o.Suffix,
		typ:    rootT,
	}
	_sql, cached := insertCache.Load(k)
	if cached {
		sql = _sql.(string)
	}

	var (
		w          strings.Builder
		scanStruct func(parentV reflect.Value, parentT reflect.Type)
		dedupMap   = dedupMapPool.Get().(map[string]struct{})
	)
	defer func() {
		for k := range dedupMap {
			delete(dedupMap, k)
		}
		dedupMapPool.Put(dedupMap)
	}()
	scanStruct = func(parentV reflect.Value, parentT reflect.Type) {
		type desc struct {
			reflect.Value
			reflect.Type
		}

		var (
			embedded []desc
			l        = parentT.NumField()
		)
		for i := 0; i < l; i++ {
			var (
				f               = parentT.Field(i)
				split           = strings.Split(f.Tag.Get("db"), ",")
				tag             = split[0]
				name            string
				convertToString bool
			)
			for _, s := range split[1:] {
				if s == "string" {
					convertToString = true
				}
			}
			switch tag {
			case "-":
				continue
			case "":
				name = f.Name
			default:
				name = tag
			}

			v := parentV.Field(i)
			if f.Anonymous {
				embedded = append(embedded, desc{
					v,
					f.Type,
				})
				continue
			}

			if _, ok := dedupMap[name]; ok {
				continue
			}

			if !cached {
				if len(dedupMap) != 0 {
					w.WriteByte(',')
				}

				// Do not quote names without specified tags to preserve case
				// insensitivity
				if tag != "" {
					w.WriteByte('"')
				}
				w.WriteString(name)
				if tag != "" {
					w.WriteByte('"')
				}
			}
			dedupMap[name] = struct{}{}
			val := v.Interface()
			if convertToString {
				// Consistently convert the value type to not allow any external
				// reflection to chose inconsistent branches
				if v.Type().Kind() == reflect.Ptr && v.IsNil() {
					val = (*string)(nil)
				} else {
					val = fmt.Sprint(val)
				}
			}
			args = append(args, val)
		}

		for _, d := range embedded {
			scanStruct(d.Value, d.Type)
		}
	}

	if !cached {
		if o.Prefix != "" {
			w.WriteString(o.Prefix)
			w.WriteByte(' ')
		}
		fmt.Fprintf(&w, `INSERT INTO "%s" (`, o.Table)
	}

	scanStruct(reflect.ValueOf(o.Data), rootT)

	if !cached {
		w.WriteString(") VALUES (")
		var tmp []byte
		for i := 0; i < len(dedupMap); i++ {
			if i != 0 {
				w.WriteByte(',')
			}
			w.WriteByte('$')
			if i < 9 {
				w.WriteByte(byte(i) + '0' + 1) // Avoids allocation
			} else {
				tmp = strconv.AppendUint(tmp[:0], uint64(i+1), 10)
				w.Write(tmp)
			}
		}
		w.WriteByte(')')

		if o.Suffix != "" {
			w.WriteByte(' ')
			w.WriteString(o.Suffix)
		}

		sql = w.String()
		insertCache.Store(k, sql)
	}

	return
}
