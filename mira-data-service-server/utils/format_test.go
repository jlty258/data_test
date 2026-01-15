package utils

import (
	"fmt"
	"strings"
	"testing"
)

func TestPreviewSQL_CompressOnly(t *testing.T) {
	in := " SELECT   a,   b \nFROM   t \n WHERE  x = 1 "
	got := PreviewSQL(in, 1000)
	want := "SELECT a, b FROM t WHERE x = 1"
	if got != want {
		t.Fatalf("compress failed\n got: %q\nwant: %q", got, want)
	}
}

func TestPreviewSQL_Truncate(t *testing.T) {
	// 构造较长 SQL，包含多余空白
	raw := "SELECT   " + strings.Repeat("x", 120) + "\nFROM t"
	compressed := strings.Join(strings.Fields(raw), " ")

	limit := 100
	got := PreviewSQL(raw, limit)
	t.Logf("got: %q", got)

	// 期望：前缀=压缩后前limit个字符；后缀为函数固定格式
	suffix := fmt.Sprintf("... (truncated, len=%d)", len(compressed))
	if !strings.HasSuffix(got, suffix) {
		t.Fatalf("missing suffix, got: %q", got)
	}
	prefix := compressed[:limit]
	if !strings.HasPrefix(got, prefix) {
		t.Fatalf("prefix mismatch\n got: %q\nwant prefix: %q", got, prefix)
	}
	if len(got) != len(prefix)+len(suffix) {
		t.Fatalf("length mismatch: got=%d want=%d", len(got), len(prefix)+len(suffix))
	}
}
