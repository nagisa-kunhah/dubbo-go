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

package imports_test

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

import (
	"github.com/apache/dubbo-go-extensions/cluster/metrics"

	"github.com/stretchr/testify/require"
)

import (
	"dubbo.apache.org/dubbo-go/v3/cluster/directory/static"
	"dubbo.apache.org/dubbo-go/v3/common"
	"dubbo.apache.org/dubbo-go/v3/common/constant"
	"dubbo.apache.org/dubbo-go/v3/common/extension"
	_ "dubbo.apache.org/dubbo-go/v3/imports"
	"dubbo.apache.org/dubbo-go/v3/protocol/base"
	"dubbo.apache.org/dubbo-go/v3/protocol/invocation"
	"dubbo.apache.org/dubbo-go/v3/protocol/protocolwrapper"
	"dubbo.apache.org/dubbo-go/v3/protocol/result"
)

func TestAdaptiveServiceRegistrationUsesExtensions(t *testing.T) {
	filterFactory, ok := extension.GetFilter(constant.AdaptiveServiceProviderFilterKey)
	require.True(t, ok)
	cluster, err := extension.GetCluster(constant.ClusterKeyAdaptiveService)
	require.NoError(t, err)
	for _, tc := range []struct {
		value any
		path  string
	}{
		{filterFactory, "filter/adaptivesvc"},
		{cluster, "cluster/cluster/adaptivesvc"},
		{extension.GetLoadbalance(constant.LoadBalanceKeyP2C), "cluster/loadbalance/p2c"},
	} {
		require.Equal(t, "github.com/apache/dubbo-go-extensions/"+tc.path,
			reflect.TypeOf(tc.value).Elem().PkgPath())
	}
}

type migrationBusinessInvoker struct {
	*base.BaseInvoker
	invoke func(base.Invocation) result.Result
}

func (i *migrationBusinessInvoker) Invoke(_ context.Context, invocation base.Invocation) result.Result {
	return i.invoke(invocation)
}

func migrationProvider(t *testing.T, path string, port int, handler func(base.Invocation) result.Result) base.Invoker {
	t.Helper()
	url, err := common.NewURL(fmt.Sprintf("dubbo://127.0.0.1:%d/%s?service.filter=padasvc&loadbalance=p2c", port, path))
	require.NoError(t, err)
	return protocolwrapper.BuildInvokerChain(&migrationBusinessInvoker{
		BaseInvoker: base.NewBaseInvoker(url), invoke: handler,
	}, constant.ServiceFilterKey)
}

func migrationResponse(inv base.Invocation) result.Result {
	res := &result.RPCResult{Rest: "ok"}
	res.SetAttachments(inv.Attachments())
	return res
}

func TestAdaptiveServiceCapacityRoundTripAndP2C(t *testing.T) {
	first := migrationProvider(t, t.Name()+"A", 21001, migrationResponse)
	second := migrationProvider(t, t.Name()+"B", 21002, migrationResponse)
	cluster, err := extension.GetCluster(constant.ClusterKeyAdaptiveService)
	require.NoError(t, err)
	caller := cluster.Join(static.NewDirectory([]base.Invoker{first}))
	inv := invocation.NewRPCInvocation("Echo", nil, nil)
	response := caller.Invoke(context.Background(), inv)
	require.NoError(t, response.Error())
	require.Equal(t, "ok", response.Result())
	require.Equal(t, "1", inv.GetAttachmentWithDefaultValue(constant.AdaptiveServiceEnabledKey, ""))
	remaining, err := strconv.ParseUint(response.Attachment(constant.AdaptiveServiceRemainingKey, "").(string), 10, 64)
	require.NoError(t, err)
	require.Positive(t, remaining)
	require.Equal(t, "0", response.Attachment(constant.AdaptiveServiceInflightKey, ""))
	recorded, err := metrics.LocalMetrics.GetMethodMetrics(first.GetURL(), "Echo", metrics.HillClimbing)
	require.NoError(t, err)
	require.Equal(t, remaining, recorded)

	lb := extension.GetLoadbalance(constant.LoadBalanceKeyP2C)
	providers := []base.Invoker{first, second}
	require.Same(t, second, lb.Select(providers, inv), "probe the provider without metrics")
	require.NoError(t, metrics.LocalMetrics.SetMethodMetrics(second.GetURL(), "Echo", metrics.HillClimbing, uint64(5)))
	require.Same(t, first, lb.Select(providers, inv))
	require.NoError(t, metrics.LocalMetrics.SetMethodMetrics(first.GetURL(), "Echo", metrics.HillClimbing, uint64(0)))
	require.Same(t, second, lb.Select(providers, inv))
	require.NoError(t, metrics.LocalMetrics.SetMethodMetrics(second.GetURL(), "Echo", metrics.HillClimbing, nil))
	require.Same(t, second, lb.Select(providers, inv), "nil capacity must not panic")
}

func TestAdaptiveServiceRejectsExcessConcurrentCalls(t *testing.T) {
	const requests = 80
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	var entered atomic.Int64
	provider := migrationProvider(t, t.Name(), 21003, func(inv base.Invocation) result.Result {
		entered.Add(1)
		<-release
		return migrationResponse(inv)
	})
	cluster, err := extension.GetCluster(constant.ClusterKeyAdaptiveService)
	require.NoError(t, err)
	caller := cluster.Join(static.NewDirectory([]base.Invoker{provider}))
	completed := make(chan result.Result, requests)
	start := make(chan struct{})
	for range requests {
		go func() {
			<-start
			completed <- caller.Invoke(context.Background(), invocation.NewRPCInvocation("Echo", nil, nil))
		}()
	}
	close(start)
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for range requests - 50 {
		select {
		case res := <-completed:
			require.ErrorContains(t, res.Error(), "adaptive service interrupted: reach limitation")
		case <-deadline.C:
			t.Fatal("timed out waiting for excess calls to be rejected")
		}
	}
	require.Eventually(t, func() bool { return entered.Load() == 50 }, time.Second, time.Millisecond)
	unblock()
	for range 50 {
		select {
		case res := <-completed:
			require.NoError(t, res.Error())
		case <-deadline.C:
			t.Fatal("timed out waiting for admitted calls to finish")
		}
	}
	res := caller.Invoke(context.Background(), invocation.NewRPCInvocation("Echo", nil, nil))
	require.NoError(t, res.Error(), "capacity must be released after responses")
	require.Equal(t, "0", res.Attachment(constant.AdaptiveServiceInflightKey, ""))
}
