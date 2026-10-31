/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

import (
	adaptiveclient "github.com/apache/dubbo-go-extensions/cluster/cluster/adaptivesvc"
	capacity "github.com/apache/dubbo-go-extensions/cluster/metrics"
	adaptiveprovider "github.com/apache/dubbo-go-extensions/filter/adaptivesvc"
	"github.com/apache/dubbo-go-extensions/filter/adaptivesvc/limiter"

	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/types/known/wrapperspb"
)

import (
	"dubbo.apache.org/dubbo-go/v3/client"
	"dubbo.apache.org/dubbo-go/v3/common"
	"dubbo.apache.org/dubbo-go/v3/common/constant"
	"dubbo.apache.org/dubbo-go/v3/common/extension"
	"dubbo.apache.org/dubbo-go/v3/protocol"
	tri "dubbo.apache.org/dubbo-go/v3/protocol/triple/triple_protocol"
)

type AdaptiveMigrationService struct{}

func (*AdaptiveMigrationService) Echo(_ context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	return wrapperspb.String(req.Value), nil
}

func TestAdaptiveServiceConfigurationCompatibility(t *testing.T) {
	previous := limiter.VerboseEnabled()
	t.Cleanup(func() { limiter.SetVerbose(previous) })
	for _, tc := range []struct {
		name    string
		options []ServerOption
		filters int
		verbose bool
	}{
		{"disabled", nil, 0, false},
		{"legacy", []ServerOption{WithServerAdaptiveService(), WithServerAdaptiveServiceVerbose()}, 1, true},
		{"typed", []ServerOption{WithExtension(adaptiveprovider.WithAdaptiveService(adaptiveprovider.WithVerbose(true)))}, 1, true},
		{"yaml", []ServerOption{SetServerExtensionConfigs(map[string]any{
			"adaptive-service": map[string]any{"provider": map[string]any{"verbose": true}},
		})}, 1, true},
		{"mixed", []ServerOption{WithServerAdaptiveService(), WithExtension(adaptiveprovider.WithAdaptiveService())}, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			limiter.SetVerbose(false)
			srv, err := NewServer(tc.options...)
			require.NoError(t, err)
			require.Equal(t, tc.verbose, limiter.VerboseEnabled())
			name := "com.example.AdaptiveConfig." + tc.name
			require.NoError(t, srv.RegisterService(&AdaptiveMigrationService{}, WithInterface(name), WithNotRegister()))
			svc := srv.GetServiceOptionsByInterfaceName(name)
			filters := strings.Split(svc.getUrlMap().Get(constant.ServiceFilterKey), ",")
			count := 0
			for _, filter := range filters {
				if filter == constant.AdaptiveServiceProviderFilterKey {
					count++
				}
			}
			require.Equal(t, tc.filters, count)
		})
	}
	_, err := NewServer(WithServerAdaptiveServiceVerbose())
	require.Error(t, err, "legacy validation must still reject verbose without adaptive service")
}

func TestAdaptiveServiceTripleRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options []ServerOption
	}{
		{"legacy", []ServerOption{WithServerAdaptiveService()}},
		{"typed", []ServerOption{WithExtension(adaptiveprovider.WithAdaptiveService())}},
		{"mixed", []ServerOption{WithServerAdaptiveService(), WithExtension(adaptiveprovider.WithAdaptiveService())}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			port := testFreePort(t)
			opts := append([]ServerOption{}, tc.options...)
			opts = append(opts, WithServerProtocol(protocol.WithTriple(), protocol.WithIp("127.0.0.1"), protocol.WithPort(port)))
			srv, err := NewServer(opts...)
			require.NoError(t, err)
			name := "com.example.AdaptiveMigration." + tc.name
			service := &AdaptiveMigrationService{}
			info := &common.ServiceInfo{
				InterfaceName: name,
				ServiceType:   service,
				Methods: []common.MethodInfo{{
					Name: "Echo", Type: constant.CallUnary,
					ReqInitFunc: func() any { return &wrapperspb.StringValue{} },
					MethodFunc: func(ctx context.Context, args []any, handler any) (any, error) {
						res, err := handler.(*AdaptiveMigrationService).Echo(ctx, args[0].(*wrapperspb.StringValue))
						return tri.NewResponse(res), err
					},
				}},
			}
			require.NoError(t, srv.Register(service, info, WithInterface(name), WithNotRegister()))
			require.NoError(t, srv.exportServices(context.Background()))
			t.Cleanup(func() { extension.GetProtocol(constant.TriProtocol).Destroy() })

			require.Eventually(t, func() bool {
				conn, err := net.DialTimeout("tcp", "127.0.0.1:"+strconv.Itoa(port), 100*time.Millisecond)
				if err != nil {
					return false
				}
				_ = conn.Close()
				return true
			}, 5*time.Second, 10*time.Millisecond)

			consumerOption := adaptiveclient.WithClientAdaptiveService()
			if tc.name == "legacy" {
				consumerOption = func(opts *client.ClientOptions) {
					opts.Consumer.AdaptiveService = true
				}
			}
			cli, err := client.NewClient(consumerOption)
			require.NoError(t, err)
			var ref *client.ReferenceOptions
			conn, err := cli.DialWithInfo(name, &client.ClientInfo{InterfaceName: name, MethodNames: []string{"Echo"}},
				client.WithURL("tri://127.0.0.1:"+strconv.Itoa(port)),
				func(opts *client.ReferenceOptions) { ref = opts },
			)
			require.NoError(t, err)
			t.Cleanup(func() { ref.GetInvoker().Destroy() })
			require.Equal(t, constant.ClusterKeyAdaptiveService, ref.Reference.Cluster)
			require.Equal(t, constant.LoadBalanceKeyP2C, ref.Reference.Loadbalance)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			response := &wrapperspb.StringValue{}
			var headers, trailers http.Header
			require.NoError(t, conn.CallUnary(ctx, []any{wrapperspb.String("migration")}, response, "Echo",
				client.WithResponseHeader(&headers), client.WithResponseTrailer(&trailers)))
			require.Equal(t, "migration", response.Value)
			require.NotEmpty(t, headers.Get(constant.AdaptiveServiceRemainingKey)+trailers.Get(constant.AdaptiveServiceRemainingKey),
				"provider must publish capacity over the wire")
			url, err := common.NewURL(fmt.Sprintf("tri://127.0.0.1:%d/%s", port, name))
			require.NoError(t, err)
			remaining, err := capacity.LocalMetrics.GetMethodMetrics(url, "Echo", capacity.HillClimbing)
			require.NoError(t, err, "capacity attachments must survive the Triple transport")
			require.IsType(t, uint64(0), remaining)
			require.Positive(t, remaining.(uint64))

			require.NoError(t, capacity.LocalMetrics.SetMethodMetrics(url, "Echo", capacity.HillClimbing, uint64(0)))
			require.NoError(t, conn.CallUnary(ctx, []any{wrapperspb.String("without-metadata-targets")}, response, "Echo"))
			require.Equal(t, "without-metadata-targets", response.Value)
			remaining, err = capacity.LocalMetrics.GetMethodMetrics(url, "Echo", capacity.HillClimbing)
			require.NoError(t, err)
			require.Positive(t, remaining.(uint64), "ordinary calls must update capacity without header/trailer options")
		})
	}
}
