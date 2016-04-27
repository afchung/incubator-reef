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
using System.IO;
using System.Linq;
using System.Text;
using NSubstitute;
using Org.Apache.REEF.IO.DataCache;
using Org.Apache.REEF.IO.DataCache.DataMovers;
using Org.Apache.REEF.IO.DataCache.DataRepresentations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;
using Xunit;
using Xunit.Sdk;

namespace Org.Apache.REEF.IO.Tests
{
    /// <summary>
    /// Tests for <see cref="DataCache{T, TRemoteRep}"/>.
    /// </summary>
    public sealed class TestDataCache 
    {
        [Fact]
        public void TestMaterializeFromRemote()
        {
            var tc = new TestContext();
            var cache = tc.GetBasicDataCache();
            var materialized = cache.Materialize();
            Assert.Equal(materialized, TestContext.ExpectedString);
        }

        [Fact]
        public void TestMaterializeAndCacheFromRemote()
        {
            var tc = new TestContext();
            var cache = tc.GetBasicDataCache();
            Assert.Equal(TestContext.ExpectedString, cache.MaterializeAndCache(false));
            Assert.Equal(CacheLevelConstants.InMemoryMaterialized, cache.CacheLevel);
        }

        [Fact]
        public void TestMaterializeAndCacheFromMemStream()
        {
            var tc = new TestContext();
            var cache = tc.GetDataCacheWithRegistration();
            Assert.Equal(CacheLevelConstants.InMemoryAsStream, cache.CacheFromRemote(CacheLevelConstants.InMemoryAsStream));
            Assert.Equal(TestContext.ExpectedString, cache.MaterializeAndCache(false));
            Assert.Equal(CacheLevelConstants.InMemoryMaterialized, cache.CacheLevel);
        }

        [Fact]
        public void TestRegisterRepresentersAndMovers()
        {
            var tc = new TestContext();
            var cache = tc.GetBasicDataCache();
            cache.RegisterCacheRepresenter(
                CacheRepresenter<string, MemoryStreamsDataRepresentation<string>, TestRemoteDataRepresentation>
                    .WithCacheLevel(CacheLevelConstants.InMemoryAsStream));

            cache.RegisterDataMover(new TestRemoteToMemoryStreamDataMover());
            cache.CacheFromRemote(CacheLevelConstants.InMemoryAsStream);
            Assert.Equal(CacheLevelConstants.InMemoryAsStream, cache.CacheLevel);
        }

        [Fact]
        public void TestRegisterMoverFailsOnNonExistingRepresenters()
        {
            var tc = new TestContext();
            var cache = tc.GetBasicDataCache();
            Assert.Throws<ArgumentException>(() => cache.RegisterDataMover(new TestRemoteToMemoryStreamDataMover()));
        }

        [Fact]
        public void TestRegisterMoverFailsOnTypeMismatch()
        {
            var tc = new TestContext();
            var cache = tc.GetBasicDataCache();
            cache.RegisterCacheRepresenter(
                CacheRepresenter<string, MemoryStreamsDataRepresentation<string>, TestRemoteDataRepresentation>
                    .WithCacheLevel(CacheLevelConstants.InMemoryAsStream));

            Assert.Throws<ArgumentException>(() => cache.RegisterDataMover(new TestWrongLevelDataMover()));
        }

        [Fact]
        public void TestCacheToLowerLevelReturnsLowerLevel()
        {
            var tc = new TestContext();
            var cache = tc.GetDataCacheWithRegistration();
            Assert.Equal(CacheLevelConstants.Remote, cache.CacheLevel);
            Assert.Equal(TestFakeDataRepresentation.Level, cache.CacheFromRemote(TestFakeDataRepresentation.Level));
            Assert.Equal(CacheLevelConstants.InMemoryAsStream, cache.CacheFromRemote(CacheLevelConstants.InMemoryAsStream));
            Assert.Equal(CacheLevelConstants.InMemoryMaterialized, cache.CacheFromRemote(CacheLevelConstants.InMemoryMaterialized));
            Assert.Equal(CacheLevelConstants.InMemoryMaterialized, cache.CacheLevel);
            Assert.Equal(
                new List<int>
                {
                    CacheLevelConstants.InMemoryMaterialized,
                    CacheLevelConstants.InMemoryAsStream, 
                    TestFakeDataRepresentation.Level,
                    CacheLevelConstants.Remote,
                },
                cache.PresentCacheLevels);
        }

        [Fact]
        public void TestCacheToHigherLevelReturnsLowerLevel()
        {
            var tc = new TestContext();
            var cache = tc.GetDataCacheWithRegistration();
            Assert.Equal(CacheLevelConstants.InMemoryMaterialized, cache.CacheFromRemote(CacheLevelConstants.InMemoryMaterialized));
            Assert.Equal(CacheLevelConstants.InMemoryMaterialized, cache.CacheFromRemote(CacheLevelConstants.InMemoryAsStream));
            Assert.Equal(CacheLevelConstants.InMemoryMaterialized, cache.CacheFromRemote(TestFakeDataRepresentation.Level));
            Assert.Equal(CacheLevelConstants.InMemoryMaterialized, cache.CacheFromRemote(CacheLevelConstants.Remote));
            Assert.Equal(2, cache.PresentCacheLevels.Count);
        }

        [Fact]
        public void TestCleanCacheWhileLoweringLevelReturnsRightLevel()
        {
            var tc = new TestContext();
            var cache = tc.GetDataCacheWithRegistration();
            Assert.Equal(TestFakeDataRepresentation.Level, cache.CacheFromLowestLevel(TestFakeDataRepresentation.Level, true));
            Assert.Equal(TestFakeDataRepresentation.Level, cache.CacheLevel);
            Assert.Equal(2, cache.PresentCacheLevels.Count);
            Assert.Equal(CacheLevelConstants.InMemoryAsStream, cache.CacheFromLowestLevel(CacheLevelConstants.InMemoryAsStream, true));
            Assert.Equal(CacheLevelConstants.InMemoryAsStream, cache.CacheLevel);
            Assert.Equal(2, cache.PresentCacheLevels.Count);
            Assert.Equal(CacheLevelConstants.Remote, cache.CleanCacheAtLevel(cache.CacheLevel));
            Assert.Equal(CacheLevelConstants.Remote, cache.CacheLevel);
            Assert.Equal(1, cache.PresentCacheLevels.Count);
        }

        [Fact]
        public void TestCleanCacheClearsAllUpperCacheLevels()
        {
            var tc = new TestContext();
            var cache = tc.GetDataCacheWithRegistration();
            Assert.Equal(TestFakeDataRepresentation.Level, cache.CacheFromLowestLevel(TestFakeDataRepresentation.Level));
            Assert.Equal(CacheLevelConstants.InMemoryAsStream, cache.CacheFromLowestLevel(CacheLevelConstants.InMemoryAsStream));
            Assert.Equal(CacheLevelConstants.InMemoryMaterialized, cache.CacheFromLowestLevel(CacheLevelConstants.InMemoryMaterialized, true));
            Assert.Equal(CacheLevelConstants.Remote, cache.CleanCacheAtLevel(CacheLevelConstants.InMemoryMaterialized));
            Assert.Equal(CacheLevelConstants.Remote, cache.CacheLevel);
            Assert.Equal(1, cache.PresentCacheLevels.Count);
        }

        [Fact]
        public void TestClearCache()
        {
            var tc = new TestContext();
            var cache = tc.GetDataCacheWithRegistration();
            cache.CacheFromRemote(TestFakeDataRepresentation.Level);
            cache.CacheFromRemote(CacheLevelConstants.InMemoryAsStream);
            cache.CacheFromRemote(CacheLevelConstants.InMemoryMaterialized);
            Assert.Equal(4, cache.PresentCacheLevels.Count);
            Assert.Equal(CacheLevelConstants.InMemoryMaterialized, cache.CacheLevel);
            cache.Clear();
            Assert.Equal(CacheLevelConstants.Remote, cache.CacheLevel);
            Assert.Equal(1, cache.PresentCacheLevels.Count);
        }

        [Fact]
        public void TestCleanCacheJumpLevels()
        {
            var tc = new TestContext();
            var cache = tc.GetDataCacheWithRegistration();
            Assert.Equal(TestFakeDataRepresentation.Level, cache.CacheFromRemote(TestFakeDataRepresentation.Level));
            Assert.Equal(CacheLevelConstants.InMemoryMaterialized, cache.CacheFromRemote(CacheLevelConstants.InMemoryMaterialized));
            Assert.Equal(TestFakeDataRepresentation.Level, cache.CleanCacheAtLevel(CacheLevelConstants.InMemoryMaterialized));
        }

        [Fact]
        public void TestCachedDoesNotInvokeDataMover()
        {
            var tc = new TestContext();
            var cache = tc.GetBasicDataCache();
            cache.RegisterCacheRepresenter(CacheRepresenter<string, MemoryStreamsDataRepresentation<string>, TestRemoteDataRepresentation>
                .WithCacheLevel(CacheLevelConstants.InMemoryAsStream));
            cache.RegisterDataMover(tc.MockRemoteToMemoryStreamsDataMover);
            cache.CacheFromRemote(CacheLevelConstants.InMemoryAsStream);
            tc.MockRemoteToMemoryStreamsDataMover.ReceivedWithAnyArgs(1)
                .Move(new TestRemoteDataRepresentation(TestContext.ExpectedString));
            tc.MockRemoteToMemoryStreamsDataMover.ClearReceivedCalls();
            cache.CacheFromRemote(CacheLevelConstants.InMemoryAsStream);
            tc.MockRemoteToMemoryStreamsDataMover.ReceivedWithAnyArgs(0)
                .Move(new TestRemoteDataRepresentation(TestContext.ExpectedString));
            tc.MockRemoteToMemoryStreamsDataMover.ClearReceivedCalls();
            cache.Clear();
            cache.CacheFromRemote(CacheLevelConstants.InMemoryMaterialized);
            tc.MockRemoteToMemoryStreamsDataMover.ReceivedWithAnyArgs(0)
                .Move(new TestRemoteDataRepresentation(TestContext.ExpectedString));
        }

        [Fact]
        public void TestErrorThrowingDataMoverDoesNotCorruptState()
        {
            var tc = new TestContext();
            var cache = tc.GetBasicDataCache();
            cache.RegisterCacheRepresenter(CacheRepresenter<string, MemoryStreamsDataRepresentation<string>, TestRemoteDataRepresentation>
                .WithCacheLevel(CacheLevelConstants.InMemoryAsStream));
            cache.RegisterCacheRepresenter(CacheRepresenter<string, TestFakeDataRepresentation, TestRemoteDataRepresentation>
                .WithCacheLevel(TestFakeDataRepresentation.Level));

            cache.RegisterDataMover(new TestRemoteToFakeDataMover());
            cache.RegisterDataMover(new TestErrorThrowingDataMover());

            Assert.Equal(TestFakeDataRepresentation.Level, cache.CacheFromLowestLevel(TestFakeDataRepresentation.Level));
            Assert.Throws<InvalidOperationException>(() => cache.CacheFromLowestLevel(CacheLevelConstants.InMemoryAsStream));
            Assert.Equal(TestFakeDataRepresentation.Level, cache.CacheLevel);
            Assert.Equal(new List<int>
            {
                TestFakeDataRepresentation.Level,
                CacheLevelConstants.Remote
            }, cache.PresentCacheLevels);
        }

        private sealed class TestContext
        {
            public const string ExpectedString = "abc";

            public readonly RemoteInitializer<string, TestRemoteDataRepresentation> RemoteInitializer;

            public readonly IDataMover<string, TestRemoteDataRepresentation, MemoryStreamsDataRepresentation<string>> MockRemoteToMemoryStreamsDataMover 
                = Substitute.For<IDataMover<string, TestRemoteDataRepresentation, MemoryStreamsDataRepresentation<string>>>();

            public TestContext()
            {
                RemoteInitializer = new TestRemoteInitializer(ExpectedString);
                
                MockRemoteToMemoryStreamsDataMover.CacheLevelFrom.ReturnsForAnyArgs(CacheLevelConstants.Remote);
                MockRemoteToMemoryStreamsDataMover.CacheLevelTo.ReturnsForAnyArgs(CacheLevelConstants.InMemoryAsStream);
                MockRemoteToMemoryStreamsDataMover.Move(new TestRemoteDataRepresentation(ExpectedString))
                    .ReturnsForAnyArgs(new MemoryStreamsDataRepresentation<string>(
                        new TestSerializer(), new[] { new MemoryStream(Encoding.UTF8.GetBytes(ExpectedString)) }));
            }

            public DataCache<string, TestRemoteDataRepresentation> GetBasicDataCache()
            {
                var injector = TangFactory.GetTang().NewInjector();
                injector.BindVolatileInstance(GenericType<RemoteInitializer<string, TestRemoteDataRepresentation>>.Class, RemoteInitializer);
                return injector.GetInstance<DataCache<string, TestRemoteDataRepresentation>>();
            }

            public DataCache<string, TestRemoteDataRepresentation> GetDataCacheWithRegistration()
            {
                var cache = GetBasicDataCache();
                cache.RegisterCacheRepresenter(
                    CacheRepresenter<string, MemoryStreamsDataRepresentation<string>, TestRemoteDataRepresentation>
                        .WithCacheLevel(CacheLevelConstants.InMemoryAsStream));

                cache.RegisterCacheRepresenter(CacheRepresenter<string, TestFakeDataRepresentation, TestRemoteDataRepresentation>
                    .WithCacheLevel(TestFakeDataRepresentation.Level));

                cache.RegisterDataMover(new TestRemoteToMemoryStreamDataMover());

                cache.RegisterDataMover(new TestRemoteToFakeDataMover());

                cache.RegisterDataMover(new TestFakeToMemoryStreamsDataMover());

                return cache;
            }
        }

        private sealed class TestRemoteToMemoryStreamDataMover :
            RemoteDataMover<string, TestRemoteDataRepresentation, MemoryStreamsDataRepresentation<string>>
        {
            private readonly ISerializer<string> _serializer = new TestSerializer();

            public override int CacheLevelTo
            {
                get { return CacheLevelConstants.InMemoryAsStream; }
            }

            public override MemoryStreamsDataRepresentation<string> Move(TestRemoteDataRepresentation representationFrom)
            {
                var stream = new MemoryStream(Encoding.UTF8.GetBytes(representationFrom.ToData()));
                return new MemoryStreamsDataRepresentation<string>(_serializer, new[] { stream });
            }
        }

        private sealed class TestRemoteInitializer : RemoteInitializer<string, TestRemoteDataRepresentation>
        {
            private readonly string _string;

            public TestRemoteInitializer(string expectedString)
            {
                _string = expectedString;
            }

            protected override TestRemoteDataRepresentation ConstructRemoteRepresentation()
            {
                return new TestRemoteDataRepresentation(_string);
            }
        }

        private sealed class TestSerializer : ISerializer<string>
        {
            public void Serialize(string value, Stream stream)
            {
                var bytes = Encoding.UTF8.GetBytes(value);
                stream.Write(bytes, 0, bytes.Length);
            }

            public void Serialize(string value, IEnumerable<Stream> streams)
            {
                var stream = streams.Single();
                Serialize(value, stream);
            }

            public string Deserialize(Stream stream)
            {
                var memStream = new MemoryStream();
                stream.CopyTo(memStream);
                return Encoding.UTF8.GetString(memStream.ToArray());
            }

            public string Deserialize(IEnumerable<Stream> streams)
            {
                var stream = streams.Single();
                return Deserialize(stream);
            }
        }

        /// <summary>
        /// This is public for mocking.
        /// </summary>
        public sealed class TestRemoteDataRepresentation : IRemoteDataRepresentation<string>
        {
            private readonly string _string;

            public TestRemoteDataRepresentation(string expectedString)
            {
                _string = expectedString;
            }

            public string ToData()
            {
                return _string;
            }

            public void Dispose()
            {
            }
        }

        private sealed class TestRemoteToFakeDataMover : 
            RemoteDataMover<string, TestRemoteDataRepresentation, TestFakeDataRepresentation>
        {
            public override int CacheLevelTo
            {
                get { return TestFakeDataRepresentation.Level; }
            }

            public override TestFakeDataRepresentation Move(TestRemoteDataRepresentation representationFrom)
            {
                return new TestFakeDataRepresentation(representationFrom.ToData());
            }
        }

        private sealed class TestFakeToMemoryStreamsDataMover :
            IDataMover<string, TestFakeDataRepresentation, MemoryStreamsDataRepresentation<string>>
        {
            private readonly ISerializer<string> _serializer = new TestSerializer();

            public int CacheLevelFrom
            {
                get { return TestFakeDataRepresentation.Level; }
            }

            public int CacheLevelTo
            {
                get { return CacheLevelConstants.InMemoryAsStream; }
            }

            public MemoryStreamsDataRepresentation<string> Move(TestFakeDataRepresentation representationFrom)
            {
                var stream = new MemoryStream(Encoding.UTF8.GetBytes(representationFrom.ToData()));
                return new MemoryStreamsDataRepresentation<string>(_serializer, new[] { stream });
            }
        }

        private sealed class TestErrorThrowingDataMover :
            IDataMover<string, TestFakeDataRepresentation, MemoryStreamsDataRepresentation<string>>
        {
            private readonly ISerializer<string> _serializer = new TestSerializer();

            public int CacheLevelFrom
            {
                get { return TestFakeDataRepresentation.Level; }
            }

            public int CacheLevelTo
            {
                get { return CacheLevelConstants.InMemoryAsStream; }
            }

            public MemoryStreamsDataRepresentation<string> Move(TestFakeDataRepresentation representationFrom)
            {
                throw new Exception();
            }
        }

        private sealed class TestFakeDataRepresentation : IDataRepresentation<string>
        {
            public const int Level = 10000000;

            private readonly string _string;

            public TestFakeDataRepresentation(string expectedString)
            {
                _string = expectedString;
            }

            public string ToData()
            {
                return _string;
            }
            
            public void Dispose()
            {
            }
        }

        private sealed class TestWrongLevelDataMover :
            IDataMover<string, IDataRepresentation<string>, IDataRepresentation<string>>
        {
            public int CacheLevelFrom
            {
                get { return CacheLevelConstants.Remote; }
            }

            public int CacheLevelTo
            {
                get { return CacheLevelConstants.InMemoryAsStream;  }
            }

            public IDataRepresentation<string> Move(IDataRepresentation<string> representationFrom)
            {
                throw new NotImplementedException();
            }
        }
    }
}