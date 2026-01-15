package service

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"data-service/common"
	"data-service/log"

	ds "data-service/generated/datasource"
	"data-service/mocks"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/golang/mock/gomock"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setup() {
	viper.Set("LoggerConfig.Level", "debug") // 模拟配置项
	viper.Set("TestConfig", "true")
	viper.AutomaticEnv()        // 启用自动环境变量加载（如果有需要）
	viper.SetConfigType("yaml") // 确保配置类型是 yaml（如果依赖 yaml 类型）
	viper.SetConfigFile("")     // 禁用文件读取，避免默认路径读取配置文件

	log.InitLogger()
}

func TestMain(m *testing.M) {
	setup() // 调用 setup 方法
	m.Run() // 运行所有测试
}

func TestCSVStreamingService_createArrowRecordFromCSV(t *testing.T) {
	// 创建服务实例
	service := &CSVStreamingService{}

	tests := []struct {
		name        string
		records     [][]string
		schema      *arrow.Schema
		columns     []string
		expectError bool
		checkRecord func(t *testing.T, record arrow.Record)
	}{
		{
			name: "正常转换CSV数据到Arrow记录",
			records: [][]string{
				{"John", "25", "New York", "Engineer"},
				{"Jane", "30", "San Francisco", "Designer"},
				{"Mike", "28", "Seattle", "Product Manager"},
			},
			schema: arrow.NewSchema(
				[]arrow.Field{
					{Name: "name", Type: arrow.BinaryTypes.String},
					{Name: "age", Type: arrow.PrimitiveTypes.Int32},
					{Name: "city", Type: arrow.BinaryTypes.String},
					{Name: "job", Type: arrow.BinaryTypes.String},
				},
				nil,
			),
			columns:     []string{"name", "age", "city", "job"},
			expectError: false,
			checkRecord: func(t *testing.T, record arrow.Record) {
				assert.Equal(t, int64(3), record.NumRows())
				assert.Equal(t, int64(4), record.NumCols())

				// 检查第一列（姓名）
				nameCol := record.Column(0).(*array.String)
				assert.Equal(t, "John", nameCol.Value(0))
				assert.Equal(t, "Jane", nameCol.Value(1))
				assert.Equal(t, "Mike", nameCol.Value(2))

				// 检查第二列（年龄）
				ageCol := record.Column(1).(*array.Int32)
				assert.Equal(t, int32(25), ageCol.Value(0))
				assert.Equal(t, int32(30), ageCol.Value(1))
				assert.Equal(t, int32(28), ageCol.Value(2))
			},
		},
		{
			name:    "空记录集",
			records: [][]string{},
			schema: arrow.NewSchema(
				[]arrow.Field{
					{Name: "name", Type: arrow.BinaryTypes.String},
					{Name: "age", Type: arrow.PrimitiveTypes.Int32},
				},
				nil,
			),
			columns:     []string{"name", "age"},
			expectError: false,
			checkRecord: func(t *testing.T, record arrow.Record) {
				assert.Equal(t, int64(0), record.NumRows())
				assert.Equal(t, int64(2), record.NumCols())
			},
		},
		{
			name: "行数据不足时自动填充空值",
			records: [][]string{
				{"John", "25"},
				{"Jane"},
				{"Mike", "28", "Seattle"},
			},
			schema: arrow.NewSchema(
				[]arrow.Field{
					{Name: "name", Type: arrow.BinaryTypes.String},
					{Name: "age", Type: arrow.PrimitiveTypes.Int32},
					{Name: "city", Type: arrow.BinaryTypes.String},
				},
				nil,
			),
			columns:     []string{"name", "age", "city"},
			expectError: false,
			checkRecord: func(t *testing.T, record arrow.Record) {
				assert.Equal(t, int64(3), record.NumRows())
				assert.Equal(t, int64(3), record.NumCols())

				// 检查第三列（城市），不足的行应该填充空字符串
				cityCol := record.Column(2).(*array.String)
				assert.Equal(t, "", cityCol.Value(0))        // 第一行没有城市数据
				assert.Equal(t, "", cityCol.Value(1))        // 第二行没有城市数据
				assert.Equal(t, "Seattle", cityCol.Value(2)) // 第三行有城市数据
			},
		},
		{
			name: "包含空字符串值",
			records: [][]string{
				{"John", "", "New York"},
				{"", "30", ""},
				{"Mike", "28", "Seattle"},
			},
			schema: arrow.NewSchema(
				[]arrow.Field{
					{Name: "name", Type: arrow.BinaryTypes.String},
					{Name: "age", Type: arrow.PrimitiveTypes.Int32},
					{Name: "city", Type: arrow.BinaryTypes.String},
				},
				nil,
			),
			columns:     []string{"name", "age", "city"},
			expectError: false,
			checkRecord: func(t *testing.T, record arrow.Record) {
				assert.Equal(t, int64(3), record.NumRows())

				// 检查空字符串值
				nameCol := record.Column(0).(*array.String)
				assert.Equal(t, "John", nameCol.Value(0))
				assert.Equal(t, "", nameCol.Value(1))
				assert.Equal(t, "Mike", nameCol.Value(2))
			},
		},
		{
			name:    "nil记录集",
			records: nil,
			schema: arrow.NewSchema(
				[]arrow.Field{
					{Name: "name", Type: arrow.BinaryTypes.String},
				},
				nil,
			),
			columns:     []string{"name"},
			expectError: false,
			checkRecord: func(t *testing.T, record arrow.Record) {
				assert.Equal(t, int64(0), record.NumRows())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 执行测试
			record, err := service.createArrowRecordFromCSV(tt.records, tt.schema, tt.columns)

			// 检查错误
			if tt.expectError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, record)

			// 执行自定义检查
			if tt.checkRecord != nil {
				tt.checkRecord(t, record)
			}

			// 清理资源
			record.Release()
		})
	}
}

// 测试边界情况
func TestCSVStreamingService_createArrowRecordFromCSV_EdgeCases(t *testing.T) {
	service := &CSVStreamingService{}

	t.Run("单行数据", func(t *testing.T) {
		records := [][]string{{"John", "25", "New York"}}
		schema := arrow.NewSchema(
			[]arrow.Field{
				{Name: "name", Type: arrow.BinaryTypes.String},
				{Name: "age", Type: arrow.PrimitiveTypes.Int32},
				{Name: "city", Type: arrow.BinaryTypes.String},
			},
			nil,
		)
		columns := []string{"name", "age", "city"}

		record, err := service.createArrowRecordFromCSV(records, schema, columns)
		require.NoError(t, err)
		defer record.Release()

		assert.Equal(t, int64(1), record.NumRows())
		assert.Equal(t, int64(3), record.NumCols())
	})

	t.Run("单列数据", func(t *testing.T) {
		records := [][]string{
			{"John"},
			{"Jane"},
			{"Mike"},
		}
		schema := arrow.NewSchema(
			[]arrow.Field{
				{Name: "name", Type: arrow.BinaryTypes.String},
			},
			nil,
		)
		columns := []string{"name"}

		record, err := service.createArrowRecordFromCSV(records, schema, columns)
		require.NoError(t, err)
		defer record.Release()

		assert.Equal(t, int64(3), record.NumRows())
		assert.Equal(t, int64(1), record.NumCols())
	})
}

// 测试性能
func TestCSVStreamingService_createArrowRecordFromCSV_Performance(t *testing.T) {
	service := &CSVStreamingService{}

	// 创建大量测试数据
	rows := 1000
	cols := 10
	records := make([][]string, rows)
	for i := 0; i < rows; i++ {
		record := make([]string, cols)
		for j := 0; j < cols; j++ {
			record[j] = fmt.Sprintf("value_%d_%d", i, j)
		}
		records[i] = record
	}

	// 创建schema
	fields := make([]arrow.Field, cols)
	for i := 0; i < cols; i++ {
		fields[i] = arrow.Field{Name: fmt.Sprintf("col_%d", i), Type: arrow.BinaryTypes.String}
	}
	schema := arrow.NewSchema(fields, nil)

	columns := make([]string, cols)
	for i := 0; i < cols; i++ {
		columns[i] = fmt.Sprintf("col_%d", i)
	}

	// 执行性能测试
	start := time.Now()
	record, err := service.createArrowRecordFromCSV(records, schema, columns)
	duration := time.Since(start)

	require.NoError(t, err)
	defer record.Release()

	assert.Equal(t, int64(rows), record.NumRows())
	assert.Equal(t, int64(cols), record.NumCols())

	// 记录性能指标
	t.Logf("处理 %d 行 %d 列数据耗时: %v", rows, cols, duration)
}

// 测试内存分配
func TestCSVStreamingService_createArrowRecordFromCSV_MemoryAllocation(t *testing.T) {
	service := &CSVStreamingService{}

	// 创建测试数据
	records := [][]string{
		{"John", "25", "New York"},
		{"Jane", "30", "San Francisco"},
	}
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "age", Type: arrow.PrimitiveTypes.Int32},
			{Name: "city", Type: arrow.BinaryTypes.String},
		},
		nil,
	)
	columns := []string{"name", "age", "city"}

	// 记录初始内存状态
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	initialAlloc := m.TotalAlloc

	// 执行转换
	record, err := service.createArrowRecordFromCSV(records, schema, columns)
	require.NoError(t, err)

	// 记录转换后内存状态
	runtime.ReadMemStats(&m)
	afterAlloc := m.TotalAlloc

	// 清理资源
	record.Release()

	// 记录最终内存状态
	runtime.ReadMemStats(&m)
	finalAlloc := m.TotalAlloc

	t.Logf("初始内存分配: %d bytes", initialAlloc)
	t.Logf("转换后内存分配: %d bytes", afterAlloc)
	t.Logf("清理后内存分配: %d bytes", finalAlloc)
	t.Logf("转换过程内存增长: %d bytes", afterAlloc-initialAlloc)
	t.Logf("清理后内存释放: %d bytes", afterAlloc-finalAlloc)
}

func TestCSVStreamingService_ExtractDbNameFromJobInstanceId_AtWithHex_UseConst(t *testing.T) {
	service := &CSVStreamingService{}

	makeHex := func(ch string, n int) string {
		return strings.Repeat(ch, n)
	}
	N := common.SUFFIX_RANDOM_LENGTH

	tests := []struct {
		name string
		in   string
		want string
	}{
		{"合法@+N hex 小写", "db@" + makeHex("a", N), "db"},
		{"合法@+N hex 大写", "db@" + makeHex("A", N), "db"},
		{"多重@ 取最后一个", "proj@db@" + makeHex("f", N), "proj@db"},
		{"不足N位", "db@" + makeHex("a", N-1), "db@" + makeHex("a", N-1)},
		{"超过N位", "db@" + makeHex("a", N+1), "db@" + makeHex("a", N+1)},
		{"含非hex字符", "db@" + makeHex("a", N-1) + "Z", "db@" + makeHex("a", N-1) + "Z"},
		{"@在首位", "@" + makeHex("a", N), "@" + makeHex("a", N)},
		{"无@", "dbname", "dbname"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := service.extractDbNameFromJobInstanceId(tt.in)
			t.Logf("input=%q -> output=%q", tt.in, got) // 打印转化前后
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCSVStreamingService_convertCSVLinesToArrow_JSONQuotedWithComma(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDoris := mocks.NewMockIDorisService(ctrl)
	svc := &CSVStreamingService{dorisService: mockDoris}

	tableName := "mira_mid_00000_74"
	cols := []*ds.ColumnItem{
		{Name: "srv_alias0", DataType: "VARCHAR"},
	}
	mockDoris.EXPECT().
		GetDorisTableSchema(gomock.Any(), tableName).
		Return(cols, nil)

	// CSV 行：一个字段，外层双引号包裹，内部双引号翻倍；包含逗号
	lines := []string{
		`"{""k"":30,""v"":900}"`,
		`"{""k"":29,""v"":841}"`,
	}

	record, err := svc.convertCSVLinesToArrow(lines, "db@abcdef12", tableName, []string{"srv_alias0"})
	require.NoError(t, err)
	require.NotNil(t, record)
	defer record.Release()

	assert.Equal(t, int64(2), record.NumRows())
	assert.Equal(t, int64(1), record.NumCols())

	col := record.Column(0).(*array.String)
	assert.Equal(t, `{"k":30,"v":900}`, col.Value(0))
	assert.Equal(t, `{"k":29,"v":841}`, col.Value(1))
}

func TestCSVStreamingService_convertDorisTypeToArrowType_Decimal(t *testing.T) {
	service := &CSVStreamingService{}

	tests := []struct {
		name          string
		dorisType     string
		wantPrecision int32
		wantScale     int32
	}{
		{
			name:          "decimal default precision",
			dorisType:     "DECIMAL",
			wantPrecision: 38,
			wantScale:     10,
		},
		{
			name:          "decimal with precision and scale",
			dorisType:     "decimal(18,4)",
			wantPrecision: 18,
			wantScale:     4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt, err := service.convertDorisTypeToArrowType(tt.dorisType)
			require.NoError(t, err)

			dec, ok := dt.(*arrow.Decimal128Type)
			require.True(t, ok, "expected Decimal128Type, got %T", dt)
			assert.Equal(t, tt.wantPrecision, dec.Precision)
			assert.Equal(t, tt.wantScale, dec.Scale)
		})
	}
}

func Test_cleanCSVRecord_RemovesLineBreaksInPlace(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		out  []string
	}{
		{
			name: "remove \\n",
			in:   []string{"a\nb", "x\ny", "\n\n", "no-newline"},
			out:  []string{"ab", "xy", "", "no-newline"},
		},
		{
			name: "remove \\r",
			in:   []string{"a\rb", "x\ry", "\r", "x\r\ry"},
			out:  []string{"ab", "xy", "", "xy"},
		},
		{
			name: "remove \\r\\n",
			in:   []string{"a\r\nb", "x\r\ny", "\r\n", "prefix\r\nsuffix"},
			out:  []string{"ab", "xy", "", "prefixsuffix"},
		},
		{
			name: "mixed keep others",
			in:   []string{"  a \n b  ", "\rhello\r\nworld\n", "plain"},
			out:  []string{"  a  b  ", "helloworld", "plain"},
		},
		{
			name: "empty strings unchanged",
			in:   []string{"", "", ""},
			out:  []string{"", "", ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := append([]string(nil), tt.in...) // 拷贝，避免修改测试用例字面量
			cleanCSVRecord(rec)
			assert.Equal(t, tt.out, rec)
		})
	}
}
