/*
 * Copyright 2019 The Baudtime Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package baudb

import (
	"context"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/baudb/baudb/backend"
	"github.com/baudb/baudb/backend/storage"
	"github.com/baudb/baudb/logql"
	"github.com/baudb/baudb/meta"
	"github.com/baudb/baudb/msg"
	backendmsg "github.com/baudb/baudb/msg/backend"
	gatewaymsg "github.com/baudb/baudb/msg/gateway"
	"github.com/baudb/baudb/tcp"
	osutil "github.com/baudb/baudb/util/os"
	. "github.com/baudb/baudb/vars"
	"github.com/buaazp/fasthttprouter"
	"github.com/go-kit/kit/log/level"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/pprofhandler"
)

type tcpServerObserver struct {
	gateway   *Gateway
	storage   *storage.Storage
	heartbeat *meta.Heartbeat
}

func (obs *tcpServerObserver) OnStart() error {
	if obs.gateway != nil {
		if err := meta.Init(); err != nil {
			level.Error(Logger).Log("msg", "failed to init meta data", "err", err)
			return err
		}
	}
	if obs.heartbeat != nil {
		if err := obs.heartbeat.Start(); err != nil {
			level.Error(Logger).Log("msg", "failed to start heartbeat", "err", err)
			return err
		}
	}
	level.Info(Logger).Log("msg", "baudb started")
	return nil
}

func (obs *tcpServerObserver) OnStop() error {
	if obs.heartbeat != nil {
		obs.heartbeat.Stop()
		obs.storage.Close()
	}
	level.Info(Logger).Log("msg", "baudb shutdown")
	return nil
}

func (obs *tcpServerObserver) OnAccept(tcpConn *net.TCPConn) *tcp.ReadWriteLoop {
	level.Debug(Logger).Log("msg", "new connection accepted", "remoteAddr", tcpConn.RemoteAddr())

	tcpConn.SetNoDelay(true)
	tcpConn.SetKeepAlive(true)
	tcpConn.SetKeepAlivePeriod(60 * time.Second)
	tcpConn.SetReadBuffer(1024 * 1024)
	tcpConn.SetWriteBuffer(1024 * 1024)

	return tcp.NewReadWriteLoop(tcpConn, func(ctx context.Context, req tcp.Message, reqBytes []byte) tcp.Message {
		raw := req.GetRaw()
		response := tcp.Message{Opaque: req.GetOpaque()}

		switch request := raw.(type) {
		case *gatewaymsg.AddRequest:
			err := obs.gateway.Ingest(request)
			if err != nil {
				response.SetRaw(&msg.GeneralResponse{
					Status:  msg.StatusCode_Failed,
					Message: err.Error(),
				})
			} else {
				return tcp.EmptyMsg
			}
		case *gatewaymsg.SelectRequest:
			response.SetRaw(obs.gateway.SelectQuery(request))
		case *gatewaymsg.SeriesLabelsRequest:
			response.SetRaw(obs.gateway.SeriesLabels(request))
		case *gatewaymsg.LabelValuesRequest:
			response.SetRaw(obs.gateway.LabelValues(request))
		case *backendmsg.AddRequest:
			response.SetRaw(obs.storage.HandleAddReq(request))
		case *backendmsg.SelectRequest:
			response.SetRaw(obs.storage.HandleSelectReq(request))
		case *backendmsg.LabelValuesRequest:
			response.SetRaw(obs.storage.HandleLabelValuesReq(request))
		case *backendmsg.SlaveOfCommand:
			//response.SetRaw(obs.storage.ReplicateManager.HandleSlaveOfCmd(request))
		case *backendmsg.SyncHandshake:
			//response.SetRaw(obs.storage.ReplicateManager.HandleSyncHandshake(request))
		case *backendmsg.SyncHeartbeat:
			//response.SetRaw(obs.storage.ReplicateManager.HandleHeartbeat(request))
		case *backendmsg.AdminCmdInfo:
			info, err := obs.storage.Info(true)
			if err != nil {
				response.SetRaw(&msg.GeneralResponse{Status: msg.StatusCode_Failed, Message: err.Error()})
			} else {
				response.SetRaw(&msg.GeneralResponse{Status: msg.StatusCode_Succeed, Message: info.String()})
			}
		case *backendmsg.AdminCmdJoinCluster:
			shardID, err := obs.storage.JoinCluster(request.Addr)
			if err != nil {
				response.SetRaw(&msg.GeneralResponse{Status: msg.StatusCode_Failed, Message: err.Error()})
			} else {
				response.SetRaw(&msg.GeneralResponse{Status: msg.StatusCode_Succeed, Message: shardID})
			}
		}

		return response
	})
}

func Run() {
	var (
		localStorage *storage.Storage
		heartbeat    *meta.Heartbeat
		gateway      *Gateway
		router       = fasthttprouter.New()
	)

	if Cfg.Storage != nil {
		var err error
		localStorage, err = storage.New()
		if err != nil {
			level.Error(Logger).Log("msg", "failed to init storage", "err", err)
			return
		}

		heartbeat = meta.NewHeartbeat(time.Duration(Cfg.Storage.StatReport.SessionExpireTTL), time.Duration(Cfg.Storage.StatReport.HeartbeartInterval), func() (meta.Node, error) {
			stat, err := localStorage.Info(false)
			return stat.Node, err
		})

		router.GET("/joinCluster", func(ctx *fasthttp.RequestCtx) {
			addr := ctx.QueryArgs().Peek("addr")
			localStorage.JoinCluster(string(addr))
		})
		router.GET("/stat", func(ctx *fasthttp.RequestCtx) {
			if arg := ctx.QueryArgs().Peek("reset"); arg != nil {
				localStorage.OpStat.Reset()
			}
			stat, err := localStorage.Info(true)
			if err != nil {
				ctx.Error(err.Error(), http.StatusInternalServerError)
			} else {
				ctx.SuccessString("text/plain", stat.String())
			}
		})
	}

	if Cfg.Gateway != nil {
		gateway = &Gateway{
			Backend:     backend.NewFanout(localStorage),
			QueryEngine: logql.NewEngine(time.Duration(Cfg.Gateway.QueryEngine.Timeout)),
		}

		router.GET("/api/v1/query", gateway.HttpSelectQuery)
		router.POST("/api/v1/query", gateway.HttpSelectQuery)
		router.GET("/api/v1/series", gateway.HttpSeriesLabels)
		router.POST("/api/v1/series", gateway.HttpSeriesLabels)
		router.GET("/api/v1/label/:name/values", gateway.HttpLabelValues)
		router.POST("/api/v1/label/:name/values", gateway.HttpLabelValues)
	}

	httpServer := &fasthttp.Server{}
	go func() {
		httpServer.Handler = fasthttp.CompressHandler(func(ctx *fasthttp.RequestCtx) {
			if strings.HasPrefix(string(ctx.Path()), "/debug/pprof") {
				pprofhandler.PprofHandler(ctx)
			} else {
				router.Handler(ctx)
			}
		})
		if err := httpServer.ListenAndServe(":" + Cfg.HttpPort); err != nil {
			level.Error(Logger).Log("msg", "failed to start http server for baudb", "err", err)
			return
		}
	}()

	tcpServer := tcp.NewTcpServer(Cfg.TcpPort, Cfg.MaxConn, &tcpServerObserver{
		gateway:   gateway,
		storage:   localStorage,
		heartbeat: heartbeat,
	})
	go tcpServer.Run()

	osutil.HandleSignals(func(sig os.Signal) bool {
		level.Warn(Logger).Log("msg", "trapped signal", "signal", sig)
		if sig == syscall.SIGTERM || sig == syscall.SIGINT {
			go httpServer.Shutdown()
			tcpServer.Shutdown()
			return false
		}
		return true
	})
}
