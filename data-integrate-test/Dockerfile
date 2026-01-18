FROM golang:1.23.0 as builder

RUN go env -w GO111MODULE=on
RUN go env -w GOPROXY=https://goproxy.cn,direct

WORKDIR /home/workspace

# 先复制 go.mod 和 go.sum（利用 Docker 缓存层）
# 如果依赖没有变化，这一层会被缓存，避免重复下载
COPY go.mod go.sum ./
RUN go mod download

# 复制 vendor 目录（如果存在，优先使用本地依赖）
# 注意：需要先运行 go mod vendor 生成 vendor 目录
# 如果 vendor 不存在，构建会失败，请先运行: make vendor
COPY vendor ./vendor

# 复制项目源代码
COPY . .

# 编译 proto 文件（需要安装 protoc）
# RUN apt-get update && apt-get install -y protobuf-compiler
# RUN cd proto && protoc --go_out=. --go_opt=paths=source_relative *.proto

# 使用 vendor 模式构建（如果 vendor 存在）
# -mod=vendor 会优先使用 vendor 目录中的依赖，避免网络下载
RUN go build -mod=vendor -o data-integrate-test main.go

FROM ubuntu:24.04

RUN apt-get update && \
    apt-get install -y tzdata ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV TZ=Asia/Shanghai

WORKDIR /home/workspace

COPY --from=builder /home/workspace/data-integrate-test /home/workspace/bin/data-integrate-test
COPY --from=builder /home/workspace/config /home/workspace/config/
COPY --from=builder /home/workspace/templates /home/workspace/templates/
RUN chmod +x /home/workspace/bin/data-integrate-test

CMD ["/home/workspace/bin/data-integrate-test"]

