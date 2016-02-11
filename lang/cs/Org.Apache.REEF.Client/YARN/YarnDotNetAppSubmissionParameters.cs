// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
// 
//   http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using Org.Apache.REEF.Client.YARN.Parameters;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Wake.Remote.Parameters;

namespace Org.Apache.REEF.Client.YARN
{
    [Unstable("New API.")]
    public sealed class YarnDotNetAppSubmissionParameters
    {
        private readonly int _tcpPortRangeStart;
        private readonly int _tcpPortRangeCount;
        private readonly int _tcpPortRangeTryCount;
        private readonly int _driverMemorySizeMB;
        private readonly int _driverRestartEvaluatorRecoverySeconds;

        [Inject]
        private YarnDotNetAppSubmissionParameters(
            [Parameter(typeof(TcpPortRangeStart))] int tcpPortRangeStart,
            [Parameter(typeof(TcpPortRangeCount))] int tcpPortRangeCount,
            [Parameter(typeof(TcpPortRangeTryCount))] int tcpPortRangeTryCount,
            [Parameter(typeof(DriverMemorySizeMB))] int driverMemorySizeMB, 
            [Parameter(typeof(DriverBridgeConfigurationOptions.DriverRestartEvaluatorRecoverySeconds))] int driverRestartEvaluatorRecoverySeconds)
        {
            _tcpPortRangeStart = tcpPortRangeStart;
            _tcpPortRangeCount = tcpPortRangeCount;
            _tcpPortRangeTryCount = tcpPortRangeTryCount;
            _driverMemorySizeMB = driverMemorySizeMB;
            _driverRestartEvaluatorRecoverySeconds = driverRestartEvaluatorRecoverySeconds;
        }

        public int TcpPortRangeStart
        {
            get { return _tcpPortRangeStart; }
        }

        public int TcpPortRangeCount
        {
            get
            {
                return _tcpPortRangeCount;
            }
        }

        public int TcpPortRangeTryCount
        {
            get
            {
                return _tcpPortRangeTryCount;
            }
        }

        public int DriverMemorySizeMB
        {
            get 
            {
                return _driverMemorySizeMB;
            }
        }

        public int DriverRestartEvaluatorRecoverySeconds
        {
            get
            {
                return _driverRestartEvaluatorRecoverySeconds;
            }
        }

        public sealed class Builder
        {
            private ConfigurationModule _confModule;

            public static Builder NewBuilder()
            {
                return new Builder();
            }

            public Builder SetTcpPortRangeStart(int tcpPortRangeStart)
            {
                _confModule = _confModule.Set(
                    YarnDotNetAppSubmissionParametersConfiguration.TcpPortRangeStart,
                    tcpPortRangeStart.ToString());

                return this;
            }

            public Builder SetTcpPortRangeCount(int tcpPortRangeCount)
            {
                _confModule = _confModule.Set(
                    YarnDotNetAppSubmissionParametersConfiguration.TcpPortRangeCount,
                    tcpPortRangeCount.ToString());

                return this;
            }

            public Builder SetTcpPortRangeTryCount(int tcpPortRangeTryCount)
            {
                _confModule = _confModule.Set(
                    YarnDotNetAppSubmissionParametersConfiguration.TcpPortRangeTryCount,
                    tcpPortRangeTryCount.ToString());

                return this;
            }

            public Builder SetDriverMemorySizeMB(int driverMemorySizeMB)
            {
                _confModule = _confModule.Set(
                    YarnDotNetAppSubmissionParametersConfiguration.DriverMemoryMB,
                    driverMemorySizeMB.ToString());

                return this;
            }

            public Builder SetDriverRestartEvaluatorRecoverySeconds(int driverRestartEvaluatorRecoverySeconds)
            {
                _confModule = _confModule.Set(
                   YarnDotNetAppSubmissionParametersConfiguration.DriverRestartEvaluatorRecoverySeconds,
                   driverRestartEvaluatorRecoverySeconds.ToString());

                return this;
            }

            public YarnDotNetAppSubmissionParameters Build()
            {
                return TangFactory.GetTang().NewInjector(_confModule.Build()).GetInstance<YarnDotNetAppSubmissionParameters>();
            }

            private Builder()
            {
                _confModule = YarnDotNetAppSubmissionParametersConfiguration.ConfigurationModule;
            }
        }
    }
}