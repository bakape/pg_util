package pg_util

import "testing"

func TestTestBuildInsert(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name, sql     string
		opts          InsertOpts
		args          []interface{}
		before, after func()
	}

	type inner struct {
		F3 int
	}

	type innerOverlapping struct {
		F2 int
	}

	ch := make(chan struct{})

	cases := [...]testCase{
		{
			name: "simple",
			opts: InsertOpts{
				Table: "t1",
				Data: struct {
					F1 string
					F2 int
				}{"aaa", 1},
			},
			sql:  `INSERT INTO "t1" (F1,F2) VALUES ($1,$2)`,
			args: []interface{}{"aaa", 1},
			after: func() {
				close(ch)
			},
		},
		{
			name: "cached",
			opts: InsertOpts{
				Table: "t1",
				Data: struct {
					F1 string
					F2 int
				}{"aaa", 1},
			},
			sql:  `INSERT INTO "t1" (F1,F2) VALUES ($1,$2)`,
			args: []interface{}{"aaa", 1},
			before: func() {
				// Ensure this test always runs after "simple"
				<-ch
			},
		},
		{
			name: "with name tag and string tag",
			opts: InsertOpts{
				Table: "t1",
				Data: struct {
					F1 string `db:"field_1"`
					F2 int    `db:"field_2,string"`
				}{"aaa", 1},
			},
			sql:  `INSERT INTO "t1" ("field_1","field_2") VALUES ($1,$2)`,
			args: []interface{}{"aaa", 1},
		},
		{
			name: "with only string tag",
			opts: InsertOpts{
				Table: "t1",
				Data: struct {
					F1 string `db:"field_1"`
					F2 int    `db:",string"`
				}{"aaa", 1},
			},
			sql:  `INSERT INTO "t1" ("field_1",F2) VALUES ($1,$2)`,
			args: []interface{}{"aaa", 1},
		},
		{
			name: "with skipped field",
			opts: InsertOpts{
				Table: "t1",
				Data: struct {
					F1 string
					F2 int
					F3 int `db:"-"`
				}{"aaa", 1, 1},
			},
			sql:  `INSERT INTO "t1" (F1,F2) VALUES ($1,$2)`,
			args: []interface{}{"aaa", 1},
		},
		{
			name: "with prefix and suffix",
			opts: InsertOpts{
				Table: "t1",
				Data: struct {
					F1 string
					F2 int
				}{"aaa", 1},
				Prefix: "with v as (select 1)",
				Suffix: "returning f1",
			},
			sql:  `with v as (select 1) INSERT INTO "t1" (F1,F2) VALUES ($1,$2) returning f1`,
			args: []interface{}{"aaa", 1},
		},
		{
			name: "with embedded struct",
			opts: InsertOpts{
				Table: "t1",
				Data: struct {
					F1 string
					F2 int
					inner
				}{"aaa", 1, inner{3}},
			},
			sql:  `INSERT INTO "t1" (F1,F2,F3) VALUES ($1,$2,$3)`,
			args: []interface{}{"aaa", 1, 3},
		},
		{
			name: "with embedded struct override",
			opts: InsertOpts{
				Table: "t2",
				Data: struct {
					innerOverlapping
					F1 string
					F2 int
				}{innerOverlapping{3}, "aaa", 1},
			},
			sql:  `INSERT INTO "t2" (F1,F2) VALUES ($1,$2)`,
			args: []interface{}{"aaa", 1},
		},
		{
			name: "with many args",
			opts: InsertOpts{
				Table: "t1",
				Data: struct {
					F1 string
					F2 int
					F3,
					F4,
					F5,
					F6,
					F7,
					F8,
					F9,
					F10 int
				}{"aaa", 1, 2, 3, 4, 5, 6, 7, 8, 9},
			},
			sql:  `INSERT INTO "t1" (F1,F2,F3,F4,F5,F6,F7,F8,F9,F10) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
			args: []interface{}{"aaa", 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
	}

	run := func(c testCase) {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			if c.before != nil {
				c.before()
			}

			q, args := BuildInsert(c.opts)
			if q != c.sql {
				t.Fatalf("SQL mismatch: `%s` != `%s`", q, c.sql)
			}
			if q != c.sql {
				t.Fatalf("argument list mismatch: `%+v` != `%+v`", args, c.args)
			}

			if c.after != nil {
				c.after()
			}
		})
	}

	for i := range cases {
		run(cases[i])
	}
}
