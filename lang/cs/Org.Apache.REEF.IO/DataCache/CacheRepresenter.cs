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
using Org.Apache.REEF.IO.DataCache.DataMovers;
using Org.Apache.REEF.IO.DataCache.DataRepresentations;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.IO.DataCache
{
    /// <summary>
    /// The representation of data in a Cache at a specified Cache Level.
    /// </summary>
    /// <typeparam name="T">The type of the data.</typeparam>
    /// <typeparam name="TRep">The type of the representation of the data at the current cache level.</typeparam>
    /// <typeparam name="TRemoteRep">The type of the highest remote representation of the data.</typeparam>
    public sealed class CacheRepresenter<T, TRep, TRemoteRep> : AbstractCacheRepresenter<T> 
        where TRep : class, IDataRepresentation<T> 
        where TRemoteRep : class, IRemoteDataRepresentation<T>
    {
        /// <summary>
        /// Creates a CacheRepresenter at a specified cache level.
        /// </summary>
        public static CacheRepresenter<T, TRep, TRemoteRep> WithCacheLevel(int cacheLevel)
        {
            return new CacheRepresenter<T, TRep, TRemoteRep>(cacheLevel);
        }

        private readonly int _cacheLevel;
        private Optional<TRep> _representation = Optional<TRep>.Empty();

        private CacheRepresenter(int cacheLevel)
        {
            _cacheLevel = cacheLevel;
        }

        /// <summary>
        /// A private constructor used in <see cref="DataCache"/> to create the
        /// remote CacheRepresenter. This is called with reflection, please do not delete!
        /// </summary>
        private CacheRepresenter(int cacheLevel, RemoteInitializer<T, TRemoteRep> remoteInitializer)
        {
            if (cacheLevel != CacheLevelConstants.Remote)
            {
                throw new ArgumentException("This constructor for CacheRepresenter is reserved for remote representations.");
            }

            _cacheLevel = cacheLevel;

            var remoteRep = remoteInitializer.Move(default(TRemoteRep)) as TRep;
            if (remoteRep == null)
            {
                throw new ArgumentException("The remote data representation cannot be null.");
            }

            _representation = Optional<TRep>.Of(remoteRep);
        }

        /// <summary>
        /// The cache level of the <see cref="CacheRepresenter{T,TRep,TRemoteRep}"/>.
        /// </summary>
        internal override int CacheLevel
        {
            get
            {
                return _cacheLevel;
            }
        }

        /// <summary>
        /// Populates the Cache from another Cache level with an <see cref="IDataMover{T, TRepThat, TRep}"/>.
        /// </summary>
        /// <typeparam name="TRepFrom">The data representation from the origin Cache level.</typeparam>
        /// <param name="dataMover">The data mover.</param>
        /// <param name="that">The other Cache level representation.</param>
        internal void CacheToThisLevelFromThatLevel<TRepFrom>(
            IDataMover<T, TRepFrom, TRep> dataMover,
            CacheRepresenter<T, TRepFrom, TRemoteRep> that) where TRepFrom : class, IDataRepresentation<T>
        {
            if (!that._representation.IsPresent())
            {
                var message = string.Format("Cannot cache to Cache level {0} from {1} since CacheRepresenter at level {1}", CacheLevel, that.CacheLevel);
                throw new InvalidOperationException(message);
            }

            _representation = Optional<TRep>.Of(dataMover.Move(that._representation.Value));
        }

        /// <summary>
        /// Returns true if the representation has been initialized.
        /// </summary>
        internal override bool IsPresent
        {
            get { return _representation.IsPresent(); }
        }

        /// <summary>
        /// Cleans up the Cache.
        /// </summary>
        internal override void Cleanup()
        {
            if (_representation.IsPresent())
            {
                _representation.Value.Dispose();
                _representation = Optional<TRep>.Empty();
            }
        }

        /// <summary>
        /// Materializes to the data from the Cache's representation of the Data.
        /// </summary>
        internal override T Materialize()
        {
            if (!_representation.IsPresent())
            {
                throw new InvalidOperationException(string.Format("Cache level {0} has not been instantiated yet. Please use CacheToThisLevel " +
                                                                  "or CacheToThisLevelFromThatLevel to instantiate the Cache.", CacheLevel));
            }

            return _representation.Value.ToData();
        }
    }
}