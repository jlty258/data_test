FROM golang:1.23.0 as builder

RUN go env -w GO111MODULE=on
RUN go env -w GOPROXY=https://goproxy.cn,direct

WORKDIR /home/workspace
ADD ./ /home/workspace

# 编译 proto 文件（需要安装 protoc）
# RUN apt-get update && apt-get install -y protobuf-compiler
# RUN cd proto && protoc --go_out=. --go_opt=paths=source_relative *.proto

RUN go mod download
RUN go build -o data-integrate-test main.go

FROM ubuntu:24.04

RUN apt-get update && \
    apt-get install -y tzdata ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV TZ=Asia/Shanghai

WORKDIR /home/workspace

COPY --from=builder /home/workspace/data-integrate-test /home/workspace/bin/
COPY --from=builder /home/workspace/config /home/workspace/config/
COPY --from=builder /home/workspace/templates /home/workspace/templates/

CMD ["/home/workspace/bin/data-integrate-test"]

