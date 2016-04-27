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

using Org.Apache.REEF.IO.DataCache.DataRepresentations;

namespace Org.Apache.REEF.IO.DataCache.DataMovers
{
    /// <summary>
    /// A convenience abstract class that initializes the remote data representation.
    /// Please note that the remote data representation should not cache locally!
    /// It should merely provide a way to fetch the Data from remote to local.
    /// </summary>
    public abstract class RemoteInitializer<T, TRemoteRep> : RemoteDataMover<T, TRemoteRep, TRemoteRep> 
        where TRemoteRep : class, IRemoteDataRepresentation<T>
    {
        /// <summary>
        /// Returns <see cref="CacheLevelConstants.Remote"/>.
        /// </summary>
        public override int CacheLevelTo
        {
            get { return CacheLevelConstants.Remote; }
        }

        /// <summary>
        /// Implemented to call <see cref="ConstructRemoteRepresentation"/>, which initializes
        /// the remote data representation as implemented by the user.
        /// </summary>
        public override TRemoteRep Move(TRemoteRep representationFrom)
        {
            return ConstructRemoteRepresentation();
        }

        /// <summary>
        /// Implemented by the user. Initializes a remote data representation which allows the
        /// user to fetch data from remote. <see cref="TRemoteRep"/> should contain data structures
        /// to fetch data from remote such as connection strings, endpoints, authentication mechanisms...etc,
        /// but it should not contain the data itself. It should not also attempt to cache the data after
        /// fetching it. Caching the data is the job of other <see cref="IDataRepresentation{T}"/>.
        /// </summary>
        protected abstract TRemoteRep ConstructRemoteRepresentation();
    }
}