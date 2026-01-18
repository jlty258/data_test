# 多阶段构建：编译阶段
FROM golang:latest as builder

# 配置Go依赖环境
RUN go env -w GO111MODULE=on
RUN go env -w GOPROXY=https://goproxy.cn,direct

# 设置工作目录
WORKDIR /build

# 拷贝go mod文件（利用 Docker 缓存层）
# 如果依赖没有变化，这一层会被缓存，避免重复下载
COPY go.mod go.sum* ./
RUN go mod download

# 复制 vendor 目录（如果存在，优先使用本地依赖）
# 注意：需要先运行 go mod vendor 生成 vendor 目录
COPY vendor ./vendor

# 拷贝源代码
COPY . .

# 运行 go mod tidy 确保依赖正确
RUN go mod tidy

# 构建应用（使用 vendor 模式，如果 vendor 存在）
# -mod=vendor 会优先使用 vendor 目录中的依赖，避免网络下载
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -mod=vendor -o mira-gateway-mock main.go

# 运行阶段：使用alpine减小镜像大小
FROM alpine:latest

# 安装必要的运行时依赖（包括wget用于健康检查）
RUN apk --no-cache add ca-certificates tzdata wget

# 设置时区
ENV TZ=Asia/Shanghai

# 创建非root用户
RUN addgroup -g 1000 appuser && \
    adduser -D -u 1000 -G appuser appuser

WORKDIR /app

# 从builder阶段复制二进制文件
COPY --from=builder /build/mira-gateway-mock .

# 修改文件所有者
RUN chown -R appuser:appuser /app

# 切换到非root用户
USER appuser

# 暴露端口
EXPOSE 8080

# 设置默认端口环境变量
ENV PORT=8080

# 健康检查
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:${PORT}/health || exit 1

# 启动服务
CMD ["./mira-gateway-mock"]

