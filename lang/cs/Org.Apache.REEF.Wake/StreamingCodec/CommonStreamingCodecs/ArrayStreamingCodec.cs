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
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Errors;

namespace Org.Apache.REEF.Wake.StreamingCodec.CommonStreamingCodecs
{
    public abstract class ArrayStreamingCodec<T> : IStreamingCodec<T[]>
    {
        private readonly int _sizeOfType;

        protected ArrayStreamingCodec()
        {
            _sizeOfType = Marshal.SizeOf(typeof(T));
        } 

        /// <summary>
        /// Instantiate the class from the reader.
        /// </summary>
        /// <param name="reader">The reader from which to read</param>
        /// <returns>The integer array read from the reader</returns>
        public Optional<T[]> Read(IDataReader reader)
        {
            int? length = reader.ReadInt32();
            if (length == null)
            {
                return Optional<T[]>.Empty();
            }

            byte[] buffer = new byte[_sizeOfType * length.Value];
            if (reader.Read(ref buffer, 0, buffer.Length) == 0)
            {
                throw new UnexpectedReadFormatException();
            }
            
            T[] arr = new T[length.Value];
            Buffer.BlockCopy(buffer, 0, arr, 0, buffer.Length);
            return Optional<T[]>.Of(arr);
        }

        /// <summary>
        /// Writes the integer array to the writer.
        /// </summary>
        /// <param name="obj">The integer array to be encoded</param>
        /// <param name="writer">The writer to which to write</param>
        public void Write(T[] obj, IDataWriter writer)
        {
            if (obj == null)
            {
                throw new ArgumentNullException("obj", "integer array is null");
            }

            writer.WriteInt32(obj.Length);
            byte[] buffer = new byte[_sizeOfType * obj.Length];
            Buffer.BlockCopy(obj, 0, buffer, 0, _sizeOfType * buffer.Length);
            writer.Write(buffer, 0, buffer.Length);
        }

        /// <summary>
        /// Instantiate the class from the reader.
        /// </summary>
        /// <param name="reader">The reader from which to read</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>The integer array read from the reader</returns>
        public async Task<Optional<T[]>> ReadAsync(IDataReader reader, CancellationToken token)
        {
            int? length = await reader.ReadInt32Async(token);
            if (length == null)
            {
                return Optional<T[]>.Empty();
            }

            byte[] buffer = new byte[_sizeOfType * length.Value];
            await reader.ReadAsync(buffer, 0, buffer.Length, token);
            T[] arr = new T[length.Value];
            Buffer.BlockCopy(buffer, 0, arr, 0, _sizeOfType * length.Value);
            return Optional<T[]>.Of(arr);
        }

        /// <summary>
        /// Writes the integer array to the writer.
        /// </summary>
        /// <param name="obj">The integer array to be encoded</param>
        /// <param name="writer">The writer to which to write</param>
        /// <param name="token">Cancellation token</param>
        public async Task WriteAsync(T[] obj, IDataWriter writer, CancellationToken token)
        {
            if (obj == null)
            {
                throw new ArgumentNullException("obj", "integer array is null");
            }

            await writer.WriteInt32Async(obj.Length, token);
            byte[] buffer = new byte[_sizeOfType * obj.Length];
            Buffer.BlockCopy(obj, 0, buffer, 0, _sizeOfType * obj.Length);
            await writer.WriteAsync(buffer, 0, buffer.Length, token);
        } 
    }
}