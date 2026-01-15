package common

import (
	"reflect"
	"testing"
)

func TestFilterColumnsWithRowidHandling(t *testing.T) {
	tests := []struct {
		name        string
		allColumns  []ColumnInfo
		columnList  []string
		wantColumns []ColumnInfo
		wantErr     bool
		checkErr    func(error) bool
	}{
		{
			name: "Normal case without rowid",
			allColumns: []ColumnInfo{
				{Name: "id", DataType: "INT", Nullable: false, Default: ""},
				{Name: "name", DataType: "VARCHAR", Nullable: true, Default: ""},
				{Name: "age", DataType: "INT", Nullable: true, Default: ""},
			},
			columnList: []string{"id", "name"},
			wantColumns: []ColumnInfo{
				{Name: "id", DataType: "INT", Nullable: false, Default: ""},
				{Name: "name", DataType: "VARCHAR", Nullable: true, Default: ""},
			},
			wantErr: false,
		},
		{
			name: "ColumnList contains rowid and source table has rowid",
			allColumns: []ColumnInfo{
				{Name: "id", DataType: "INT", Nullable: false, Default: ""},
				{Name: "rowid", DataType: "INT", Nullable: true, Default: ""},
				{Name: "name", DataType: "VARCHAR", Nullable: true, Default: ""},
			},
			columnList: []string{"id", "rowid", "name"},
			wantColumns: []ColumnInfo{
				{Name: "id", DataType: "INT", Nullable: false, Default: ""},
				{Name: "rowid", DataType: "INT", Nullable: true, Default: ""},
				{Name: "name", DataType: "VARCHAR", Nullable: true, Default: ""},
			},
			wantErr: false,
		},
		{
			name: "ColumnList contains rowid but source table does not have rowid",
			allColumns: []ColumnInfo{
				{Name: "id", DataType: "INT", Nullable: false, Default: ""},
				{Name: "name", DataType: "VARCHAR", Nullable: true, Default: ""},
			},
			columnList: []string{"id", "rowid", "name"},
			wantColumns: []ColumnInfo{
				{Name: "id", DataType: "INT", Nullable: false, Default: ""},
				{Name: "name", DataType: "VARCHAR", Nullable: true, Default: ""},
				{Name: "rowid", DataType: "BIGINT", Nullable: true, Default: ""},
			},
			wantErr: false,
		},
		{
			name: "ColumnList contains rowid in different case",
			allColumns: []ColumnInfo{
				{Name: "id", DataType: "INT", Nullable: false, Default: ""},
				{Name: "name", DataType: "VARCHAR", Nullable: true, Default: ""},
			},
			columnList: []string{"id", "ROWID", "name"},
			wantColumns: []ColumnInfo{
				{Name: "id", DataType: "INT", Nullable: false, Default: ""},
				{Name: "name", DataType: "VARCHAR", Nullable: true, Default: ""},
				{Name: "rowid", DataType: "BIGINT", Nullable: true, Default: ""},
			},
			wantErr: false,
		},
		{
			name: "ColumnList contains rowid in mixed case",
			allColumns: []ColumnInfo{
				{Name: "id", DataType: "INT", Nullable: false, Default: ""},
				{Name: "name", DataType: "VARCHAR", Nullable: true, Default: ""},
			},
			columnList: []string{"id", "RowId", "name"},
			wantColumns: []ColumnInfo{
				{Name: "id", DataType: "INT", Nullable: false, Default: ""},
				{Name: "name", DataType: "VARCHAR", Nullable: true, Default: ""},
				{Name: "rowid", DataType: "BIGINT", Nullable: true, Default: ""},
			},
			wantErr: false,
		},
		{
			name: "Empty columnList",
			allColumns: []ColumnInfo{
				{Name: "id", DataType: "INT", Nullable: false, Default: ""},
				{Name: "name", DataType: "VARCHAR", Nullable: true, Default: ""},
			},
			columnList: []string{},
			wantColumns: []ColumnInfo{
				{Name: "id", DataType: "INT", Nullable: false, Default: ""},
				{Name: "name", DataType: "VARCHAR", Nullable: true, Default: ""},
			},
			wantErr: false,
		},
		{
			name: "ColumnList contains duplicate rowid",
			allColumns: []ColumnInfo{
				{Name: "id", DataType: "INT", Nullable: false, Default: ""},
				{Name: "name", DataType: "VARCHAR", Nullable: true, Default: ""},
			},
			columnList: []string{"id", "rowid", "rowid", "name"},
			wantColumns: []ColumnInfo{
				{Name: "id", DataType: "INT", Nullable: false, Default: ""},
				{Name: "name", DataType: "VARCHAR", Nullable: true, Default: ""},
				{Name: "rowid", DataType: "BIGINT", Nullable: true, Default: ""},
			},
			wantErr: false,
		},
		{
			name: "Filter fails for non-rowid column",
			allColumns: []ColumnInfo{
				{Name: "id", DataType: "INT", Nullable: false, Default: ""},
				{Name: "name", DataType: "VARCHAR", Nullable: true, Default: ""},
			},
			columnList:  []string{"id", "nonexistent", "name"},
			wantColumns: nil,
			wantErr:     true,
			checkErr: func(err error) bool {
				return err != nil && (err.Error() != "" || err.Error() != "")
			},
		},
		{
			name: "Source table has rowid with different type, should keep original",
			allColumns: []ColumnInfo{
				{Name: "id", DataType: "INT", Nullable: false, Default: ""},
				{Name: "rowid", DataType: "VARCHAR", Nullable: true, Default: ""},
				{Name: "name", DataType: "VARCHAR", Nullable: true, Default: ""},
			},
			columnList: []string{"id", "rowid", "name"},
			wantColumns: []ColumnInfo{
				{Name: "id", DataType: "INT", Nullable: false, Default: ""},
				{Name: "rowid", DataType: "VARCHAR", Nullable: true, Default: ""},
				{Name: "name", DataType: "VARCHAR", Nullable: true, Default: ""},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := FilterColumnsWithRowidHandling(tt.allColumns, tt.columnList)
			if (err != nil) != tt.wantErr {
				t.Errorf("FilterColumnsWithRowidHandling() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				if tt.checkErr != nil && !tt.checkErr(err) {
					t.Errorf("FilterColumnsWithRowidHandling() error validation failed: %v", err)
				}
				return
			}
			if !reflect.DeepEqual(got, tt.wantColumns) {
				t.Errorf("FilterColumnsWithRowidHandling() = %v, want %v", got, tt.wantColumns)
			}
		})
	}
}
