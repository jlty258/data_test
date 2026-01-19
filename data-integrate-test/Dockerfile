# **********编译程序镜像**********
# 使用 golang:latest 作为基础镜像
FROM golang:latest AS builder

# 配置Go依赖环境
RUN go env -w GO111MODULE=on && \
    go env -w GOPROXY=https://goproxy.cn,direct

# 安装必要的构建工具（protobuf 用于编译 proto 文件）
RUN apt-get update && \
    apt-get install -y -qq \
    protobuf-compiler && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 安装 protobuf Go 插件（如果需要重新编译 proto）
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# 设置工作目录
WORKDIR /build

# 先复制 go.mod 和 go.sum（利用 Docker 缓存层）
# 如果依赖没有变化，这一层会被缓存，避免重复下载
COPY go.mod go.sum ./

# 下载依赖（利用缓存，依赖不常变化）
RUN go mod download

# 复制项目源文件
COPY . .

# 编译所有程序（禁用 CGO，纯 Go 编译）
# 使用 -ldflags '-w -s' 减小二进制体积（去除调试信息和符号表）
RUN CGO_ENABLED=0 go build -ldflags '-w -s' -o data-integrate-test ./main.go && \
    CGO_ENABLED=0 go build -ldflags '-w -s' -o manage-ida ./cmd/manage_ida/main.go && \
    CGO_ENABLED=0 go build -ldflags '-w -s' -o query-ida ./cmd/query_ida/main.go && \
    CGO_ENABLED=0 go build -ldflags '-w -s' -o test-clients ./cmd/test_clients/main.go

# 第二阶段，运行镜像
FROM ubuntu:22.04

# 更新软件包并安装运行时依赖（合并减少层数）
RUN apt-get update && \
    apt-get install -y -qq \
    ca-certificates \
    tzdata && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 设置时区
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 设置工作目录
WORKDIR /app

# 从builder阶段复制二进制文件（先删除可能存在的同名目录或文件）
RUN rm -rf /app/data-integrate-test /app/manage-ida /app/query-ida /app/test-clients
COPY --from=builder /build/data-integrate-test /app/data-integrate-test
COPY --from=builder /build/manage-ida /app/manage-ida
COPY --from=builder /build/query-ida /app/query-ida
COPY --from=builder /build/test-clients /app/test-clients

# 复制配置文件和模板文件
COPY --from=builder /build/config /app/config
COPY --from=builder /build/templates /app/templates

# 创建必要的目录并设置可执行权限（合并命令减少层数）
RUN mkdir -p /app/snapshots /app/reports && \
    chmod +x /app/data-integrate-test /app/manage-ida /app/query-ida /app/test-clients

# 默认运行主程序（可以通过 docker run 覆盖）
ENTRYPOINT ["/app/data-integrate-test"]
CMD ["-template", "templates/mysql/mysql_1m_8fields.yaml"]
