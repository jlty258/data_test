# 多阶段构建：编译阶段
FROM golang:latest as builder

# 配置Go依赖环境
RUN go env -w GO111MODULE=on
RUN go env -w GOPROXY=https://goproxy.cn,direct

# 安装protoc和protoc-gen-go工具
RUN apt-get update && \
    apt-get install -y protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

# 安装Go的protobuf插件
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# 设置工作目录
WORKDIR /build

# 拷贝go mod文件（利用 Docker 缓存层）
COPY go.mod go.sum ./

# 下载依赖
RUN go mod download

# 拷贝proto文件
COPY proto/ ./proto/

# 生成proto代码
RUN mkdir -p mirapb && \
    protoc --go_out=. --go_opt=module=ida-access-service-mock \
           --go-grpc_out=. --go-grpc_opt=module=ida-access-service-mock \
           -I. proto/common.proto proto/mira_ida_access_service.proto && \
    mv proto/*.pb.go mirapb/ 2>/dev/null || true && \
    ls -la mirapb/

# 拷贝源代码
COPY main.go ./

# 运行 go mod tidy 确保依赖正确
RUN go mod tidy

# 拷贝存储层代码
COPY storage/ ./storage/

# 构建应用
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ida-access-service-mock .

# 运行阶段：使用alpine减小镜像大小
FROM alpine:latest

# 安装必要的运行时依赖
RUN apk --no-cache add ca-certificates tzdata

# 设置时区
ENV TZ=Asia/Shanghai

# 创建非root用户
RUN addgroup -g 1000 appuser && \
    adduser -D -u 1000 -G appuser appuser

WORKDIR /app

# 从builder阶段复制二进制文件
COPY --from=builder /build/ida-access-service-mock .

# 修改文件所有者
RUN chown -R appuser:appuser /app

# 切换到非root用户
USER appuser

# 暴露端口
EXPOSE 9091

# 设置默认端口环境变量
ENV PORT=9091

# 启动服务
CMD ["./ida-access-service-mock"]

