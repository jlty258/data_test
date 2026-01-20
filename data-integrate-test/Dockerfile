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

# 复制项目源文件（包括 Makefile）
COPY . .

# 创建输出目录
RUN mkdir -p bin

# 编译主程序（先只编译主程序，其他工具可以后续添加）
RUN CGO_ENABLED=0 go build -ldflags '-w -s' -o bin/data-integrate-test .

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

# 从builder阶段复制二进制文件（只复制已构建的）
RUN rm -rf /app/data-integrate-test /app/manage-ida /app/test-clients /app/export-table /app/import-table /app/export-snapshots /app/import-snapshots
COPY --from=builder /build/bin/data-integrate-test /app/data-integrate-test
# 其他工具暂时不复制，如需要可以后续添加
# COPY --from=builder /build/bin/manage-ida /app/manage-ida
# COPY --from=builder /build/bin/test-clients /app/test-clients
# COPY --from=builder /build/bin/export-table /app/export-table
# COPY --from=builder /build/bin/import-table /app/import-table
# COPY --from=builder /build/bin/export-snapshots /app/export-snapshots
# COPY --from=builder /build/bin/import-snapshots /app/import-snapshots

# 复制配置文件和模板文件
COPY --from=builder /build/config /app/config
COPY --from=builder /build/templates /app/templates

# 创建必要的目录并设置可执行权限（合并命令减少层数）
RUN mkdir -p /app/snapshots /app/reports && \
    chmod +x /app/data-integrate-test

# 复制入口点脚本
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# 使用入口点脚本，支持多种使用方式：
# 1. docker run <image> /app/export-table ... (直接指定工具)
# 2. docker run <image> export-table ... (使用工具名，脚本会自动查找)
# 3. docker run <image> (显示帮助信息)
ENTRYPOINT ["/app/entrypoint.sh"]

# 默认运行主程序（可以通过 docker run 覆盖）
CMD ["/app/data-integrate-test", "-template", "templates/mysql/mysql_1m_8fields.yaml"]
