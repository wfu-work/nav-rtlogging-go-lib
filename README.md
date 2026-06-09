# nav-rtlogging-go-lib

[![Go Version](https://img.shields.io/badge/Go-1.24%2B-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

`nav-rtlogging-go-lib` 是一个面向 RTK、差分数据链路和本地网络联调场景的 Go 通信库。它封装了 NTRIP Client、NTRIP Server、轻量级 NTRIP Caster，以及基础 TCP Client/Server，帮助业务系统快速接入、转发和调试 RTCM 等实时数据流。

## 目录

- [特性](#特性)
- [适用场景](#适用场景)
- [安装](#安装)
- [快速开始](#快速开始)
- [TCP 示例](#tcp-示例)
- [API 概览](#api-概览)
- [项目结构](#项目结构)
- [开发与测试](#开发与测试)
- [贡献指南](#贡献指南)
- [许可证](#许可证)

## 特性

- 支持连接外部 NTRIP Caster，并订阅指定挂载点数据。
- 支持作为 NTRIP 数据源连接 Caster，并向挂载点推送实时数据。
- 内置轻量级 NTRIP Caster，包含数据源接入端和客户端订阅端。
- 支持自定义认证逻辑，适配不同业务系统的账号、挂载点和密码规则。
- 支持从经纬度和高程生成标准 `$GPGGA` 语句。
- 提供连接、断开、数据接收、数据量统计和网络错误等常用回调。
- 提供基础 TCP Client/Server 封装，便于本地联调和数据转发。

## 适用场景

- RTK、GNSS、差分定位系统的数据接入。
- RTCM 或类似二进制实时数据流转发。
- 本地搭建 NTRIP Caster 进行端到端联调。
- 将外部 Caster 数据桥接到内部业务服务。
- 快速构建 TCP 数据接入、监听和转发工具。

## 安装

当前仓库的模块路径为：

```go
module github.com/wfu-work/nav-rtlogging-go-lib
```

外部项目可直接安装：

```bash
go get github.com/wfu-work/nav-rtlogging-go-lib
```

使用时按模块路径导入：

```go
import "github.com/wfu-work/nav-rtlogging-go-lib/ntrip"
import "github.com/wfu-work/nav-rtlogging-go-lib/tcp"
```

## 快速开始

### 启动本地 NTRIP Caster

本地 Caster 分为两类入口：

- 数据源端口：NTRIP Server 或设备侧向该端口推送数据。
- 客户端端口：NTRIP Client 或业务侧从该端口订阅数据。

```go
package main

import (
	"log"
	"net"

	"github.com/wfu-work/nav-rtlogging-go-lib/ntrip"
)

func main() {
	sourceSide := ntrip.NewNtripCasterServer(9090)
	clientSide := ntrip.NewNtripCasterClient(9095)

	sourceSide.OnAuth(func(mount, username, password string) bool {
		return mount == password
	})
	clientSide.OnAuth(func(mount, username, password string) bool {
		return mount == username && username == password
	})

	sourceSide.OnConnect(func(key, mount string, conn net.Conn) {
		log.Println("source connected:", key, mount)
	})
	clientSide.OnConnect(func(key, mount string, conn net.Conn) {
		log.Println("client connected:", key, mount)
	})

	sourceSide.SetNtripCasterClient(clientSide)

	if err := sourceSide.Start(); err != nil {
		log.Fatal(err)
	}
	if err := clientSide.Start(); err != nil {
		log.Fatal(err)
	}

	select {}
}
```

也可以使用内置初始化方法快速启动：

```go
sourceSide, clientSide := ntrip.InitNtripCaster(9090, 9095)
_ = sourceSide
_ = clientSide
```

默认认证逻辑：

- 数据源端：`mount == password`
- 客户端端：`mount == username == password`

如需接入业务认证，请通过 `OnAuth` 覆盖默认规则。

### 订阅 NTRIP 数据

```go
package main

import (
	"log"
	"net"

	"github.com/wfu-work/nav-rtlogging-go-lib/ntrip"
)

func main() {
	client := ntrip.NewNtripClient(
		"caster.example.com",
		2101,
		"MOUNT_POINT",
		"username",
		"password",
	)

	client.OnConnect(func(key, mount string, conn net.Conn) {
		log.Println("ntrip connected:", key, mount)
	})

	client.DisConnect(func(key, mount string) {
		log.Println("ntrip disconnected:", key, mount)
		client.Retry()
	})

	client.OnNetErrorCallback(func(err error) {
		log.Println("ntrip network error:", err)
		client.Retry()
	})

	client.OnDataCallback(func(key, mount string, data []byte, extra string) {
		log.Println("receive ntrip data:", key, mount, len(data))
	})

	if err := client.Start(); err != nil {
		log.Fatal(err)
	}

	select {}
}
```

如果 Caster 要求客户端周期性上报 GGA，可使用：

```go
client := ntrip.NewNtripClientGgaExtra(
	"caster.example.com",
	2101,
	"MOUNT_POINT",
	"username",
	"password",
	32.05330802,
	119.61051377,
	10.0,
	"",
)
```

### 作为 NTRIP 数据源推送数据

```go
package main

import (
	"log"
	"net"

	"github.com/wfu-work/nav-rtlogging-go-lib/ntrip"
)

func main() {
	server := ntrip.NewNtripServer(
		"caster.example.com",
		2101,
		"MOUNT_POINT",
		"username",
		"password",
	)

	server.OnConnect(func(key, mount string, conn net.Conn) {
		log.Println("source connected:", key, mount)
		_ = ntrip.WriteData(conn, []byte("rtcm bytes"))
	})

	server.DisConnect(func(key, mount string) {
		log.Println("source disconnected:", key, mount)
	})

	if err := server.Start(); err != nil {
		log.Fatal(err)
	}

	select {}
}
```

## TCP 示例

### TCP Server

```go
package main

import (
	"log"
	"net"

	"github.com/wfu-work/nav-rtlogging-go-lib/tcp"
)

func main() {
	server := tcp.NewTcps(2101)

	server.OnConnect(func(conn net.Conn) {
		log.Println("tcp connected:", conn.RemoteAddr())
	})

	server.OnData(func(conn net.Conn, data []byte) {
		log.Println("tcp data:", conn.RemoteAddr(), len(data))
	})

	server.DisConnect(func(conn net.Conn) {
		log.Println("tcp disconnected:", conn.RemoteAddr())
	})

	if err := server.Start(); err != nil {
		log.Fatal(err)
	}

	select {}
}
```

### TCP Client

```go
package main

import (
	"log"
	"net"

	"github.com/wfu-work/nav-rtlogging-go-lib/tcp"
)

func main() {
	client := tcp.NewTcpClient("127.0.0.1", 2101)

	client.OnConnect(func(conn net.Conn) {
		log.Println("tcp connected:", conn.RemoteAddr())
	})

	client.OnData(func(conn net.Conn, data []byte) {
		log.Println("tcp data:", len(data))
	})

	client.NetError(func(err error) {
		log.Println("tcp network error:", err)
	})

	if err := client.Start(); err != nil {
		log.Fatal(err)
	}

	select {}
}
```

## API 概览

### ntrip 包

| API | 说明 |
| --- | --- |
| `NewNtripClient(host, port, mount, username, password)` | 创建普通 NTRIP 客户端。 |
| `NewNtripClientExtra(host, port, mount, username, password, extra)` | 创建带扩展字段的 NTRIP 客户端。 |
| `NewNtripClientGgaExtra(host, port, mount, username, password, latitude, longitude, altitude, extra)` | 创建可发送 GGA 的 NTRIP 客户端。 |
| `NewNtripServer(host, port, mount, username, password)` | 创建 NTRIP 数据源客户端，用于向 Caster 推送数据。 |
| `NewNtripCasterServer(port)` | 创建 Caster 数据源接入端。 |
| `NewNtripCasterClient(port)` | 创建 Caster 客户端订阅端。 |
| `InitNtripCaster(serverPort, clientPort)` | 使用默认逻辑快速初始化本地 Caster。 |
| `GenerateGGA(latitude, longitude, altitude)` | 生成 `$GPGGA` 语句。 |
| `WriteData(conn, data)` | 向指定连接写入数据。 |

### tcp 包

| API | 说明 |
| --- | --- |
| `NewTcps(port)` | 创建 TCP Server。 |
| `NewTcpClient(host, port)` | 创建 TCP Client。 |

### 常用回调

| 回调 | 说明 |
| --- | --- |
| `OnConnect` | 连接建立或认证成功后触发。 |
| `DisConnect` | 连接断开后触发。 |
| `OnData` / `OnDataCallback` | 收到业务数据后触发。 |
| `OnSize` | 收到数据时回传数据长度。 |
| `OnAuth` | 自定义认证逻辑。 |
| `OnSpeed` | 挂载点速率统计回调。 |
| `NetError` / `OnNetErrorCallback` | 网络错误回调。 |

## 项目结构

```text
.
├── ntrip/        # NTRIP Client、Server、Caster 和挂载点连接管理
├── tcp/          # TCP Client/Server 封装
├── go.mod        # Go Module 定义
├── LICENSE       # MIT License
└── README.md     # 项目说明文档
```

## 开发与测试

克隆仓库：

```bash
git clone https://github.com/wfu-work/nav-rtlogging-go-lib.git
cd nav-rtlogging-go-lib
```

运行测试：

```bash
go test ./...
```

注意：仓库中包含用于真实网络环境的手动联调用例，例如需要外部 Caster、真实账号或会通过 `select {}` 持续阻塞的测试。将其接入 CI 前，建议先为手动联调用例补充 build tag，或拆分为独立的集成测试命令。

代码格式化：

```bash
gofmt -w ntrip tcp
```

## 贡献指南

欢迎提交 Issue 和 Pull Request。建议在提交前完成以下检查：

- 保持 API 变更向后兼容，或在 PR 中明确说明破坏性变更。
- 新增网络协议行为时补充最小可复现示例或测试。
- 涉及连接、重试、认证、并发写入的改动，请说明异常场景下的行为。
- 提交前运行 `gofmt`，并尽量运行相关测试。

## 许可证

本项目基于 [MIT License](LICENSE) 开源。
