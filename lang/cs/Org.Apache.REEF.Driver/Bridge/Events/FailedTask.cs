﻿// Licensed to the Apache Software Foundation (ASF) under one
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
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using Org.Apache.REEF.Common.Avro;
using Org.Apache.REEF.Common.Exceptions;
using Org.Apache.REEF.Driver.Bridge.Clr2java;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Driver.Bridge.Events
{
    internal sealed class FailedTask : IFailedTask
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(FailedTask));
        
        private readonly BinaryFormatter _formatter = new BinaryFormatter();
        private readonly Exception _cause;
        
        public FailedTask(IFailedTaskClr2Java failedTaskClr2Java)
        {
            var serializedInfo = failedTaskClr2Java.GetFailedTaskSerializedAvro();
            var avroFailedTask = AvroJsonSerializer<AvroFailedTask>.FromBytes(serializedInfo);

            Id = avroFailedTask.identifier;
            Data = Optional<byte[]>.OfNullable(avroFailedTask.data);
            Message = avroFailedTask.message ?? "No message in Failed Task.";
            _cause = GetCause(avroFailedTask.cause, _formatter);

            // This is always empty, even in Java.
            Description = Optional<string>.Empty();
            FailedTaskClr2Java = failedTaskClr2Java;
            ActiveContextClr2Java = failedTaskClr2Java.GetActiveContext();
        }

        public Optional<string> Reason { get; set; }

        public string Id { get; private set; }

        public string Message { get; set; }

        public Optional<string> Description { get; set; }

        public Optional<byte[]> Data { get; set; }

        public Exception Cause
        {
            get { return _cause; }
        }

        [DataMember]
        private IFailedTaskClr2Java FailedTaskClr2Java { get; set; }

        [DataMember]
        private IActiveContextClr2Java ActiveContextClr2Java { get; set; }

        /// <summary>
        /// Access the context the task ran (and crashed) on, if it could be recovered.
        /// An ActiveContext is given when the task fails but the context remains alive.
        /// On context failure, the context also fails and is surfaced via the FailedContext event.
        /// Note that receiving an ActiveContext here is no guarantee that the context (and evaluator)
        /// are in a consistent state. Application developers need to investigate the reason available
        /// via getCause() to make that call.
        /// return the context the Task ran on.
        /// </summary>
        public Optional<IActiveContext> GetActiveContext()
        {
            if (ActiveContextClr2Java == null)
            {
                return Optional<IActiveContext>.Empty();
            }

            return Optional<IActiveContext>.Of(new ActiveContext(ActiveContextClr2Java));
        }

        public Exception AsError()
        {
            return Cause;
        }

        private static Exception GetCause(byte[] serializedCause, IFormatter formatter)
        {
            if (serializedCause == null)
            {
                return new JavaTaskException();
            }

            try
            {
                using (var memStream = new MemoryStream(serializedCause))
                {
                    return (Exception)formatter.Deserialize(memStream);
                }
            }
            catch (Exception exception)
            {
                Exceptions.Caught(exception, Level.Info,
                    "Exception from Task was not able to be deserialized, returning a NonSerializableException.", Logger);

                return new NonSerializableTaskException("Exception from Task was not able to be deserialized");
            }
        }
    }
}
