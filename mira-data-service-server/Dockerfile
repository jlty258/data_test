# **********编译程序镜像**********
# 使用ubuntu作为基础镜像
FROM golang:1.23.0 as builder

# 配置Go依赖环境
RUN go env -w GO111MODULE=on
RUN go env -w GOPROXY=https://goproxy.cn,direct

# 设置工作目录
WORKDIR /home/workspace

# 拷贝项目源文件
ADD ./ /home/workspace

# build data-service
RUN make build_local


# 第二阶段，运行镜像
FROM ubuntu:24.04

# 更新软件包并安装 tzdata
RUN apt-get update && \
    apt-get install -y tzdata wget curl&& \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
# 设置时区
ENV TZ=Asia/Shanghai

# 安装 keytool
RUN apt-get update && apt-get install -y openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:${PATH}"

WORKDIR /home/workspace
# 从builder阶段复制二进制文件和调试工具到最终镜像
COPY --from=builder /home/workspace/dataserver /home/workspace/bin/
COPY --from=builder /home/workspace/start.sh /home/workspace/bin/

RUN wget -O /home/workspace/bin/doris-streamloader http://192.168.3.32:8931/repository/rawrepo/data-service/doris-streamloader && \
    chmod +x /home/workspace/bin/doris-streamloader || echo "Warning: Failed to download doris-streamloader"

RUN wget -O /home/workspace/bin/doris-streamloader-arm http://192.168.3.32:8931/repository/rawrepo/data-service/doris-streamloader-arm && \
    chmod +x /home/workspace/bin/doris-streamloader-arm || echo "Warning: Failed to download doris-streamloader-arm"    

CMD ["/home/workspace/bin/dataserver", "-config", "/home/workspace/config/config.yaml"]