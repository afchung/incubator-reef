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
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Wake.Remote.Parameters;

namespace Org.Apache.REEF.Client.YARN
{
    internal sealed class YarnDotNetAppSubmissionParametersConfiguration : ConfigurationModuleBuilder
    {
        public static RequiredParameter<int> DriverMemoryMB = new RequiredParameter<int>();

        public static OptionalParameter<int> TcpPortRangeStart = new OptionalParameter<int>();

        public static OptionalParameter<int> TcpPortRangeCount = new OptionalParameter<int>();

        public static OptionalParameter<int> TcpPortRangeTryCount = new OptionalParameter<int>();

        public static OptionalParameter<int> DriverRestartEvaluatorRecoverySeconds = new OptionalParameter<int>();

        public static ConfigurationModule ConfigurationModule
        {
            get
            {
                return new YarnDotNetAppSubmissionParametersConfiguration()
                    .BindNamedParameter(GenericType<DriverMemorySizeMB>.Class, DriverMemoryMB)
                    .BindNamedParameter(GenericType<TcpPortRangeStart>.Class, TcpPortRangeStart)
                    .BindNamedParameter(GenericType<TcpPortRangeCount>.Class, TcpPortRangeCount)
                    .BindNamedParameter(GenericType<TcpPortRangeTryCount>.Class, TcpPortRangeTryCount)
                    .BindNamedParameter(GenericType<DriverBridgeConfigurationOptions.DriverRestartEvaluatorRecoverySeconds>.Class,
                        DriverRestartEvaluatorRecoverySeconds)
                    .Build();
            }
        }
    }
}