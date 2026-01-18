# 使用 Docker 编译 proto 文件
Write-Host "正在使用 Docker 编译 proto 文件..." -ForegroundColor Cyan

$workspace = (Get-Location).Path
Write-Host "工作目录: $workspace" -ForegroundColor Gray

docker run --rm `
    -v "${workspace}:/workspace" `
    -w /workspace `
    golang:latest `
    bash -c @"
        export GOPROXY=https://goproxy.cn,direct
        apt-get update -qq && apt-get install -y -qq protobuf-compiler > /dev/null 2>&1
        go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
        go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
        mkdir -p generated/mirapb
        protoc --go_out=generated --go_opt=paths=source_relative `
               --go-grpc_out=generated --go-grpc_opt=paths=source_relative `
               -Iproto proto/mira_ida_access_service.proto proto/common.proto
        echo "Proto 编译完成"
"@

if ($LASTEXITCODE -eq 0) {
    Write-Host "✓ Proto 文件编译成功！" -ForegroundColor Green
} else {
    Write-Host "✗ Proto 文件编译失败" -ForegroundColor Red
    exit 1
}

