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
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using Org.Apache.REEF.IO.DataCache.DataMovers;
using Org.Apache.REEF.IO.DataCache.DataRepresentations;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.IO.DataCache
{
    /// <summary>
    /// A class that manages the caching of data at different layers of storage medium.
    /// </summary>
    [Unstable("0.15", "API contract may change.")]
    public sealed class DataCache<T, TRemoteRep> 
        where TRemoteRep : class, IRemoteDataRepresentation<T>
    {
        private readonly IDictionary<int, Tuple<Type, AbstractCacheRepresenter<T>>> _cacheRepresenterDictionary = 
            new Dictionary<int, Tuple<Type, AbstractCacheRepresenter<T>>>();

        private readonly IDictionary<Tuple<int, int>, object> _dataMoverDictionary = 
            new Dictionary<Tuple<int, int>, object>();

        const BindingFlags BindingFlags = 
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance;

        private int _lowestCacheLevel = CacheLevelConstants.Remote;
        
        [Inject]
        private DataCache(RemoteInitializer<T, TRemoteRep> remoteInitializer)
        {
            var materializedRepresenter = CacheRepresenter<T, MaterializedDataRepresentation<T>, TRemoteRep>
                .WithCacheLevel(CacheLevelConstants.InMemoryMaterialized);

            // Get materialized representer for free.
            RegisterCacheRepresenter(materializedRepresenter);

            var initializerRepresentationType = 
                remoteInitializer.GetType().GetInterface(typeof(IDataMover<,,>).Name).GetGenericArguments()[1];

            var remoteRepresenterType = typeof(CacheRepresenter<,,>).MakeGenericType(typeof(T), initializerRepresentationType, initializerRepresentationType);

            var constructorArgs = new object[] { CacheLevelConstants.Remote, remoteInitializer };

            var remoteRepresenter = Activator.CreateInstance(
                remoteRepresenterType, BindingFlags, null, constructorArgs, CultureInfo.InvariantCulture) as CacheRepresenter<T, TRemoteRep, TRemoteRep>;

            if (remoteRepresenter == null)
            {
                throw new ArgumentException("RemoteInitializer must be of type CacheRepresenter.");
            }

            // Register the remote representer
            RegisterCacheRepresenter(remoteRepresenter);
        }

        /// <summary>
        /// Gets the lowest cache level of the data cache.
        /// </summary>
        public int CacheLevel
        {
            get { return _lowestCacheLevel; }
        }

        /// <summary>
        /// Returns all the initialized cacheLevels in sorted order.
        /// </summary>
        /// <returns></returns>
        public IReadOnlyList<int> PresentCacheLevels
        {
            get
            {
                var list = new List<int>(_cacheRepresenterDictionary.Where(entry => entry.Value.Item2.IsPresent).Select(entry => entry.Key));
                list.Sort();
                return list;
            }
        }

        /// <summary>
        /// Registers a <see cref="CacheRepresenter{T, TRep, TRemoteRep}"/>.
        /// </summary>
        public void RegisterCacheRepresenter<TRep>(CacheRepresenter<T, TRep, TRemoteRep> cacheRepresenter) 
            where TRep : class, IDataRepresentation<T>
        {
            var cacheLevel = cacheRepresenter.CacheLevel;
            if (_cacheRepresenterDictionary.ContainsKey(cacheLevel))
            {
                throw new ArgumentException(string.Format("Cache level {0} has already been registered.", cacheLevel));
            }

            _cacheRepresenterDictionary[cacheLevel] = new Tuple<Type, AbstractCacheRepresenter<T>>(
                typeof(TRep), cacheRepresenter);

            RegisterDataMover(new ToMaterializedDataMover<T, TRep>(cacheRepresenter.CacheLevel));
        }

        /// <summary>
        /// Registers a <see cref="IDataMover{T, TRepFrom, TRepTo}"/>. Note that the 
        /// <see cref="CacheRepresenter{T, TRep, TRemoteRep}"/>s at both the origin cache level 
        /// and the destination cache level must already be registered. The types of 
        /// <see cref="TRepFrom"/> and <see cref="TRepTo"/> must also match the types registered when calling
        /// <see cref="DataCache{T, TRemoteRep}.RegisterCacheRepresenter{TRep}"/>.
        /// </summary>
        public void RegisterDataMover<TRepFrom, TRepTo>(IDataMover<T, TRepFrom, TRepTo> dataMover) 
            where TRepFrom : IDataRepresentation<T> where TRepTo : IDataRepresentation<T>
        {
            if (!_cacheRepresenterDictionary.ContainsKey(dataMover.CacheLevelFrom))
            {
                throw new ArgumentException(string.Format("Cache level from {0} has not yet been registered.", dataMover.CacheLevelFrom));
            }

            if (!_cacheRepresenterDictionary.ContainsKey(dataMover.CacheLevelTo))
            {
                throw new ArgumentException(string.Format("Cache level to {0} has not yet been registered.", dataMover.CacheLevelTo));
            }

            var typeFrom = typeof(TRepFrom);
            var cacheTypeFrom = _cacheRepresenterDictionary[dataMover.CacheLevelFrom].Item1;

            if (cacheTypeFrom != typeFrom)
            {
                var message = string.Format("The from type {0} in the CacheRepresenter of level {1} is not the same as the from type {2} in the registered DataMover",
                    cacheTypeFrom, dataMover.CacheLevelFrom, typeFrom);
                throw new ArgumentException(message);
            }

            var typeTo = typeof(TRepTo);
            var cacheTypeTo = _cacheRepresenterDictionary[dataMover.CacheLevelTo].Item1;

            if (cacheTypeTo != typeTo)
            {
                var message = string.Format("The to type {0} in the CacheRepresenter of level {1} is not the same as the to type {2} in the registered DataMover",
                    cacheTypeTo, dataMover.CacheLevelTo, typeTo);
                throw new ArgumentException(message);
            }

            var rangeKey = new Tuple<int, int>(dataMover.CacheLevelFrom, dataMover.CacheLevelTo);
            if (_dataMoverDictionary.ContainsKey(rangeKey))
            {
                throw new ArgumentException(string.Format("DataMover from {0} to {1} has already been registered.", rangeKey.Item1, rangeKey.Item2));
            }

            _dataMoverDictionary[rangeKey] = dataMover;
        }

        /// <summary>
        /// Caches to <see cref="cacheLevelTo"/> from remote.
        /// </summary>
        public int CacheFromRemote(int cacheLevelTo, bool shouldCleanHigherLevelCache = false)
        {
            return Cache(cacheLevelTo, CacheLevelConstants.Remote, shouldCleanHigherLevelCache);
        }

        /// <summary>
        /// Caches to <see cref="cacheLevelTo"/> from <see cref="DataCache{T, TRemoteRep}.CacheLevel"/>.
        /// </summary>
        public int CacheFromLowestLevel(int cacheLevelTo, bool shouldCleanHigherLevelCache = false)
        {
            return Cache(cacheLevelTo, _lowestCacheLevel, shouldCleanHigherLevelCache);
        }

        /// <summary>
        /// Caches to <see cref="cacheLevelTo"/> from <see cref="cacheLevelFrom"/> with an option to clean
        /// the higher level caches.
        /// The Cache in <see cref="cacheLevelFrom"/> must already be initialized.
        /// </summary>
        public int Cache(int cacheLevelTo, int cacheLevelFrom, bool shouldCleanHigherLevelCache = false)
        {
            if (_lowestCacheLevel <= cacheLevelTo)
            {
                return _lowestCacheLevel;
            }

            Tuple<Type, AbstractCacheRepresenter<T>> cacheRepresenterFromTuple;

            if (!_cacheRepresenterDictionary.TryGetValue(cacheLevelFrom, out cacheRepresenterFromTuple))
            {
                throw new ArgumentException(string.Format("Cache level {0} was not registered.", cacheLevelFrom));
            }

            var cacheRepresenterFrom = cacheRepresenterFromTuple.Item2;

            if (!cacheRepresenterFrom.IsPresent)
            {
                throw new ArgumentException(string.Format("Cache level {0} was not yet initialized.", cacheLevelFrom));
            }

            Tuple<Type, AbstractCacheRepresenter<T>> cacheRepresenterToTuple;

            if (!_cacheRepresenterDictionary.TryGetValue(cacheLevelTo, out cacheRepresenterToTuple))
            {
                throw new ArgumentException(string.Format("Cache level {0} was not registered.", cacheLevelTo));
            }

            var cacheRepresenterTo = cacheRepresenterToTuple.Item2;

            object dataMoverObj;

            var rangeTuple = new Tuple<int, int>(cacheLevelFrom, cacheLevelTo);
            if (!_dataMoverDictionary.TryGetValue(rangeTuple, out dataMoverObj))
            {
                throw new InvalidOperationException(
                    string.Format("No DataMover registered for data movement from cache level {0} to {1}.", cacheLevelFrom, cacheLevelTo));
            }

            var dataMoverType = dataMoverObj.GetType().GetInterface(typeof(IDataMover<,,>).Name);
            if (dataMoverType == null || !dataMoverType.IsGenericType)
            {
                throw new InvalidOperationException("DataMover was not of type " + typeof(IDataMover<,,>).Name);
            }

            var dataMoverGenericArgs = dataMoverType.GetGenericArguments();
            if (dataMoverGenericArgs.Length != 3)
            {
                throw new InvalidOperationException("DataMover was not of type " + typeof(IDataMover<,,>).Name);
            }

            var moveFromType = dataMoverGenericArgs[1];
            var repToType = cacheRepresenterTo.GetType();

            var method = repToType.GetMethod("CacheToThisLevelFromThatLevel", BindingFlags);

            if (method == null)
            {
                throw new SystemException("Unable to find method CacheToThisLevelFromThatLevel.");
            }

            var genericMethod = method.MakeGenericMethod(moveFromType);
            if (genericMethod == null)
            {
                throw new InvalidOperationException(
                    string.Format(
                        "Was not able to move data from cache level {0} to {1} due to type mismatch between " +
                        "DataMover and CacheRepresenter.",
                        cacheLevelFrom,
                        cacheLevelTo));
            }

            try
            {
                genericMethod.Invoke(
                    cacheRepresenterTo,
                    BindingFlags,
                    null,
                    new[] { dataMoverObj, cacheRepresenterFrom },
                    CultureInfo.InvariantCulture);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    string.Format(
                    "Runtime error when trying to move data from cache level {0} to {1}.",
                    cacheLevelFrom,
                    cacheLevelTo), ex);
            }

            if (shouldCleanHigherLevelCache)
            {
                CleanHigherLevelCache(cacheLevelTo);
            }

            _lowestCacheLevel = cacheLevelTo;
            return _lowestCacheLevel;
        }

        /// <summary>
        /// Materializes the data without caching or cleaning the higher level cache.
        /// </summary>
        /// <returns>The data.</returns>
        public T Materialize()
        {
            return Materialize(false, false);
        }

        /// <summary>
        /// Materializes and caches the data at the materialized cache level 
        /// (<see cref="CacheLevelConstants.InMemoryMaterialized"/>.
        /// </summary>
        /// <param name="shouldCleanHigherLevelCache">
        /// Whether or not higher level caches than <see cref="CacheLevelConstants.InMemoryMaterialized"/>
        /// should be cleaned.
        /// </param>
        /// <returns>The data.</returns>
        public T MaterializeAndCache(bool shouldCleanHigherLevelCache)
        {
            return Materialize(true, shouldCleanHigherLevelCache);
        }

        /// <summary>
        /// Cleans the Cache at the specified level.
        /// </summary>
        /// <returns>The new lowest Cache level.</returns>
        public int CleanCacheAtLevel(int cacheLevel)
        {
            if (cacheLevel == CacheLevelConstants.Remote)
            {
                throw new ArgumentException("Unable to clean remote Cache level.");
            }

            Tuple<Type, AbstractCacheRepresenter<T>> cacheRepresenterTuple;
            if (!_cacheRepresenterDictionary.TryGetValue(cacheLevel, out cacheRepresenterTuple))
            {
                throw new ArgumentException(string.Format("Cache level {0} has not yet been registered.", cacheLevel));
            }

            var cacheRepresenter = cacheRepresenterTuple.Item2;
            if (cacheRepresenter.IsPresent)
            {
                cacheRepresenter.Cleanup();
            }

            _lowestCacheLevel = FindLowestPresentCacheLevel();

            return _lowestCacheLevel;
        }

        /// <summary>
        /// Clears the entire data cache.
        /// </summary>
        public void Clear()
        {
            var levels = PresentCacheLevels;
            foreach (var level in levels.Where(level => level != CacheLevelConstants.Remote))
            {
                Tuple<Type, AbstractCacheRepresenter<T>> cacheRepresenterTuple;
                if (!_cacheRepresenterDictionary.TryGetValue(level, out cacheRepresenterTuple))
                {
                    throw new SystemException(string.Format("Unable to get the Cache representer for the cache level {0}", level));
                }

                cacheRepresenterTuple.Item2.Cleanup();
            }

            _lowestCacheLevel = CacheLevelConstants.Remote;
        }

        /// <summary>
        /// Materializes and optionally caches the data at the materialized cache level,
        /// with an option to clean the higher level caches.
        /// </summary>
        /// <param name="shouldCache">Whether to cache.</param>
        /// <param name="shouldCleanHigherLevelCache">Whether to clean the higher level caches.</param>
        /// <returns>The data.</returns>
        private T Materialize(bool shouldCache, bool shouldCleanHigherLevelCache)
        {
            if (!shouldCache && shouldCleanHigherLevelCache)
            {
                throw new SystemException("Invalid state. shouldCleanHigherLevelCache can only be true if shouldCache is true.");
            }

            if (shouldCache)
            {
                Cache(CacheLevelConstants.InMemoryMaterialized, _lowestCacheLevel, shouldCleanHigherLevelCache);
            }

            Tuple<Type, AbstractCacheRepresenter<T>> cacheRepresenterTuple;
            if (!_cacheRepresenterDictionary.TryGetValue(_lowestCacheLevel, out cacheRepresenterTuple))
            {
                throw new SystemException(string.Format("Unable to get the Cache representer for the lowest cache level {0}", _lowestCacheLevel));
            }

            var cacheRepresenter = cacheRepresenterTuple.Item2;

            if (!cacheRepresenter.IsPresent)
            {
                throw new SystemException(string.Format("Cache at Cache level {0} has not yet been initialized.", cacheRepresenter.CacheLevel));
            }

            return cacheRepresenter.Materialize();
        }

        /// <summary>
        /// Finds the lowest present cache level.
        /// </summary>
        private int FindLowestPresentCacheLevel()
        {
            return _cacheRepresenterDictionary.Where(entry => entry.Value.Item2.IsPresent).Select(entry => entry.Key).Min();
        }

        /// <summary>
        /// Cleans the higher level cache.
        /// </summary>
        private void CleanHigherLevelCache(int newLevel)
        {
            var levels = PresentCacheLevels;

            if (levels.Count < 2)
            {
                throw new SystemException("There should at least be two Cache levels, the lowest cache level and the new level.");
            }

            if (_lowestCacheLevel <= newLevel)
            {
                throw new SystemException("The lowest Cache level is expected to be at a higher level than the new level.");
            }

            if (levels[0] != newLevel)
            {
                throw new SystemException("The lowest present Cache level is expected to be the new level");
            }

            if (levels[1] != _lowestCacheLevel)
            {
                throw new SystemException("The second lowest \"present\" Cache level is expected to be the lowest recorded Cache level.");
            }

            var idx = 1;

            while (_lowestCacheLevel != CacheLevelConstants.Remote)
            {
                Tuple<Type, AbstractCacheRepresenter<T>> cacheRepresenterTuple;
                if (!_cacheRepresenterDictionary.TryGetValue(_lowestCacheLevel, out cacheRepresenterTuple))
                {
                    throw new SystemException(string.Format("Unable to get the Cache representer for the cache level {0}", _lowestCacheLevel));
                }

                cacheRepresenterTuple.Item2.Cleanup();
                var prevLowest = _lowestCacheLevel;

                idx++;

                _lowestCacheLevel = levels[idx];

                if (prevLowest > _lowestCacheLevel)
                {
                    throw new SystemException("The new lowest Cache level must be higher than the previous lowest Cache level.");
                }
            }
        }
    }
}