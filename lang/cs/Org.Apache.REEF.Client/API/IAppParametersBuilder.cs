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

using System;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Client.API
{
    /// <summary>
    /// Facilitates building of job submissions
    /// </summary>
    public interface IAppParametersBuilder
    {
        /// <summary>
        /// Bake the information provided so far and return a <see cref="IAppParameters"/>
        /// </summary>
        IAppParameters Build();

        /// <summary>
        /// Make this file available to all containers
        /// </summary>
        IAppParametersBuilder AddGlobalFile(string fileName);

        /// <summary>
        /// Files specific to one container
        /// </summary>
        IAppParametersBuilder AddLocalFile(string fileName);

        /// <summary>
        /// Assemblies available to all containers
        /// </summary>
        IAppParametersBuilder AddGlobalAssembly(string fileName);

        /// <summary>
        /// Assemblies available to a specific container
        /// </summary>
        IAppParametersBuilder AddLocalAssembly(string fileName);

        /// <summary>
        /// Configuration that will be available to the driver
        /// </summary>
        IAppParametersBuilder AddDriverConfiguration(IConfiguration configuration);

        /// <summary>
        /// Find the assembly for this type and make it available to a specific container 
        /// </summary>
        IAppParametersBuilder AddLocalAssemblyForType(Type type);

        /// <summary>
        /// Find the assembly for this type and make it available to all containers
        /// </summary>
        IAppParametersBuilder AddGlobalAssemblyForType(Type type);

        /// <summary>
        /// Set driver memory in megabytes
        /// </summary>
        IAppParametersBuilder SetDriverMemory(int driverMemoryInMb);

        /// <summary>
        /// Driver config file contents (Org.Apache.REEF.Bridge.exe.config) contents
        /// Can be used to redirect assembly versions
        /// </summary>
        /// <param name="driverConfigurationFileContents">Driver configuration file contents.</param>
        /// <returns><see cref="IAppParametersBuilder"/></returns>
        IAppParametersBuilder SetDriverConfigurationFileContents(string driverConfigurationFileContents);
    }
}