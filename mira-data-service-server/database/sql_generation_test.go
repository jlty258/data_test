/*
*

	@author: shiliang
	@date: 2025/2/26
	@note:

*
*/
package database

import (
	"data-service/config"
	pb "data-service/generated/datasource"
	"strings"
	"testing"
)

func testConf() *config.DataServiceConf {
	return &config.DataServiceConf{
		OSSConfig: config.OSSConfig{
			Host:      "minio.base1",
			Port:      9000,
			AccessKey: "minio",
			SecretKey: "minio123",
		},
	}
}

func TestBuildSelectIntoOutfileSQL_BasicColumnsOnly(t *testing.T) {
	gen := &SQLGenerator{}
	req := &pb.ExportCsvFileFromDorisRequest{
		JobInstanceId: "job_1",
		TableName:     "orders",
		DbName:        "mall",
		Columns:       []string{"id", "amount"},
	}
	sql := gen.BuildSelectIntoOutfileSQL(req, testConf())

	// 基本结构
	mustContain(t, sql, `SELECT id, amount FROM mall.orders`)
	mustContain(t, sql, `INTO OUTFILE "s3://data-service/job_1/export_"`)
	mustContain(t, sql, `"s3.endpoint" = "http://minio.base1:9000"`)
	mustContain(t, sql, `"s3.region" = "us-east-1"`)
	mustContain(t, sql, `"s3.access_key" = "minio"`)
	mustContain(t, sql, `"s3.secret_key" = "minio123"`)
	mustContain(t, sql, `"use_path_style" = "true"`)
}

func TestBuildSelectIntoOutfileSQL_WithWhereEqualAndIn(t *testing.T) {
	gen := &SQLGenerator{}
	req := &pb.ExportCsvFileFromDorisRequest{
		JobInstanceId: "job_2",
		TableName:     "user_tbl",
		DbName:        "mall",
		Columns:       []string{"id", "name"},
		FilterConditions: []*pb.FilterCondition{
			{
				FieldName: "status",
				Operator:  pb.FilterOperator_EQUAL,
				FieldValue: &pb.FilterValue{
					StrValue: "active",
				},
			},
			{
				FieldName: "id",
				Operator:  pb.FilterOperator_IN_OPERATOR,
				FieldValue: &pb.FilterValue{
					IntValues: []int32{1, 2, 3},
				},
			},
		},
	}
	sql := gen.BuildSelectIntoOutfileSQL(req, testConf())

	mustContain(t, sql, `FROM mall.user_tbl WHERE status = 'active' AND id IN (1, 2, 3)`)
}

func TestBuildSelectIntoOutfileSQL_WithOrderByAscDesc(t *testing.T) {
	gen := &SQLGenerator{}
	req := &pb.ExportCsvFileFromDorisRequest{
		JobInstanceId: "job_3",
		TableName:     "join_data",
		DbName:        "mall",
		Columns:       []string{"combined_join_column"},
		SortRules: []*pb.SortRule{
			{FieldName: "combined_join_column", SortOrder: pb.SortOrder_DESC},
		},
	}
	sql := gen.BuildSelectIntoOutfileSQL(req, testConf())

	mustContain(t, sql, `ORDER BY combined_join_column DESC`)
}

func TestBuildSelectIntoOutfileSQL_WithWhereAndOrder(t *testing.T) {
	gen := &SQLGenerator{}
	req := &pb.ExportCsvFileFromDorisRequest{
		JobInstanceId: "job_4",
		TableName:     "join_data",
		DbName:        "mall",
		Columns:       []string{"combined_join_column"},
		FilterConditions: []*pb.FilterCondition{
			{
				FieldName: "age",
				Operator:  pb.FilterOperator_GREATER_THAN_OR_EQUAL,
				FieldValue: &pb.FilterValue{
					IntValue: 18,
				},
			},
		},
		SortRules: []*pb.SortRule{
			{FieldName: "combined_join_column", SortOrder: pb.SortOrder_DESC},
		},
	}
	sql := gen.BuildSelectIntoOutfileSQL(req, testConf())

	mustContain(t, sql, `WHERE age >= 18`)
	mustContain(t, sql, `ORDER BY combined_join_column DESC`)
}

func mustContain(t *testing.T, sql string, sub string) {
	t.Helper()
	if !strings.Contains(sql, sub) {
		t.Fatalf("SQL does not contain expected substring:\nwant: %s\nsql:  %s", sub, sql)
	}
}
