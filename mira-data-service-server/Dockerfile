# **********编译程序镜像**********
# 使用ubuntu作为基础镜像
FROM golang:latest as builder

# 配置Go依赖环境
RUN go env -w GO111MODULE=on && \
    go env -w GOPROXY=https://goproxy.cn,direct

# 设置工作目录
WORKDIR /home/workspace

# 先复制 go.mod 和 go.sum（利用 Docker 缓存层）
# 如果依赖没有变化，这一层会被缓存，避免重复下载
COPY mira-data-service-server/go.mod mira-data-service-server/go.sum ./

# 先复制 vendor 目录到容器中（利用缓存，vendor 不常变化）
# 注意：构建上下文应该是项目根目录
# 如果 vendor 存在，优先使用本地依赖，避免网络下载
COPY vendor /root/vendor

# 拷贝项目源文件（mira-data-service-server 目录的所有文件）
COPY mira-data-service-server /home/workspace/

# build data-service（使用 -mod=readonly 模式，利用 vendor 但允许不一致）
# 注意：go.mod 中的 replace 指令指向 /root/vendor，vendor 目录中的依赖会被使用
RUN go build -mod=readonly -ldflags '-X "chainweaver.org.cn/chainweaver/mira/mira-data-service/config.CurrentVersion=v1.2.1" -X "chainweaver.org.cn/chainweaver/mira/mira-data-service/config.BuildDateTime=$(date +%Y/%m/%d\ %H:%M:%S)" -X "chainweaver.org.cn/chainweaver/mira/mira-data-service/config.GitBranch=v3.2.0_dev" -X "chainweaver.org.cn/chainweaver/mira/mira-data-service/config.GitCommit=7920c71"' -o dataserver ./server


# 第二阶段，运行镜像
FROM ubuntu:22.04

# 更新软件包并安装所有依赖（合并减少层数）
RUN apt-get update && \
    apt-get install -y tzdata wget curl openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
# 设置时区
ENV TZ=Asia/Shanghai
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:${PATH}"

# 复制驱动文件到 Doris 需要的路径（从本地 driver 目录）
# 驱动文件不常变化，放在前面利用缓存
RUN mkdir -p /opt/apache-doris/driver
COPY driver/*.jar /opt/apache-doris/driver/
RUN chmod 644 /opt/apache-doris/driver/*.jar

WORKDIR /home/workspace
# 从builder阶段复制二进制文件和调试工具到最终镜像
COPY --from=builder /home/workspace/dataserver /home/workspace/bin/dataserver
COPY --from=builder /home/workspace/start.sh /home/workspace/bin/start.sh
# 复制 entrypoint.sh 脚本（从构建上下文）
COPY mira-data-service-server/entrypoint.sh /home/workspace/bin/entrypoint.sh
RUN chmod +x /home/workspace/bin/entrypoint.sh

# 使用 entrypoint.sh 作为入口点（它会生成配置并启动服务）
ENTRYPOINT ["/bin/bash", "/home/workspace/bin/entrypoint.sh"]
CMD ["/home/workspace/bin/dataserver", "-config", "/home/workspace/config/config.yaml"]