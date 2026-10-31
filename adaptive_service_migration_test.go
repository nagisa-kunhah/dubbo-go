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

package dubbo

import (
	"testing"
)

import (
	"github.com/apache/dubbo-go-extensions/filter/adaptivesvc/limiter"

	"github.com/stretchr/testify/require"
)

func TestAdaptiveServiceLegacyProviderConfiguration(t *testing.T) {
	previous := limiter.VerboseEnabled()
	t.Cleanup(func() { limiter.SetVerbose(previous) })
	options := defaultInstanceOptions()
	options.Provider.AdaptiveService = true
	options.Provider.AdaptiveServiceVerbose = true

	limiter.SetVerbose(false)
	require.NoError(t, options.initGlobalProvider(false))
	require.False(t, limiter.VerboseEnabled(), "validation must not activate the process-global setting")
	require.NoError(t, options.initGlobalProvider(true))
	require.True(t, limiter.VerboseEnabled())

	options.Provider.AdaptiveService = false
	require.Error(t, options.initGlobalProvider(true))
}
