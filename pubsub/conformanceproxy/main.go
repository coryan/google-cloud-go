// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub/conformanceproxy/proxypb"
	"google.golang.org/grpc"
)

const maxGRPCMessageBytes = 64 << 20 // 64 MiB (>= 32 MiB required by proxy.proto)

func main() {
	port := flag.Int("port", 0, "TCP port to listen on on 127.0.0.1 (0 for ephemeral)")
	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", *port))
	if err != nil {
		log.Fatalf("listen 127.0.0.1:%d: %v", *port, err)
	}

	tm := initTracing()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tm.Shutdown(ctx)
	}()

	grpcSrv := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxGRPCMessageBytes),
		grpc.MaxSendMsgSize(maxGRPCMessageBytes),
	)
	srv := newProxyServer(grpcSrv, tm)
	proxypb.RegisterPubSubProxyServer(grpcSrv, srv)

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		srv.initiateShutdown(5 * time.Second)
	}()

	tcpAddr, ok := lis.Addr().(*net.TCPAddr)
	if !ok {
		log.Fatalf("unexpected listener address type %T", lis.Addr())
	}
	fmt.Fprintf(os.Stdout, "READY 127.0.0.1:%d\n", tcpAddr.Port)
	_ = os.Stdout.Sync()

	if err := grpcSrv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		log.Fatalf("serve gRPC: %v", err)
	}
	srv.waitClosed()
}
