﻿/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Common.Protobuf.ReefProtocol;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Runtime.Evaluator.Task
{
    internal sealed class TaskRuntime : IObserver<ICloseEvent>, IObserver<ISuspendEvent>, IObserver<IDriverMessage>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(TaskRuntime));

        private readonly IInjector _injector;

        private readonly TaskStatus _currentStatus;

        private readonly Lazy<IDriverConnectionMessageHandler> _driverConnectionMessageHandler;

        private readonly Lazy<IDriverMessageHandler> _driverMessageHandler;

        public TaskRuntime(IInjector taskInjector, string contextId, string taskId, HeartBeatManager heartBeatManager)
        {
            _injector = taskInjector;

            var messageSources = Optional<ISet<ITaskMessageSource>>.Empty();
            try
            {
                ITaskMessageSource taskMessageSource = _injector.GetInstance<ITaskMessageSource>();
                messageSources = Optional<ISet<ITaskMessageSource>>.Of(new HashSet<ITaskMessageSource> { taskMessageSource });
            }
            catch (Exception e)
            {
                Utilities.Diagnostics.Exceptions.Caught(e, Level.Warning, "Cannot inject task message source with error: " + e.StackTrace, LOGGER);
                // do not rethrow since this is benign
            }
            try
            {
                heartBeatManager.EvaluatorSettings.NameClient = _injector.GetInstance<INameClient>();
            }
            catch (InjectionException)
            {
                // do not rethrow since user is not required to provide name client
                LOGGER.Log(Level.Warning, "Cannot inject name client from task configuration.");
            }

            _driverConnectionMessageHandler = new Lazy<IDriverConnectionMessageHandler>(() =>
            {
                try
                {
                    return _injector.GetInstance<IDriverConnectionMessageHandler>();
                }
                catch (InjectionException)
                {
                    LOGGER.Log(Level.Info, "User did not implement IDriverConnectionMessageHandler.");
                }

                return null;
            });

            _driverMessageHandler = new Lazy<IDriverMessageHandler>(() =>
            {
                try
                {
                    return _injector.GetInstance<IDriverMessageHandler>();
                }
                catch (InjectionException ie)
                {
                    Utilities.Diagnostics.Exceptions.CaughtAndThrow(ie, Level.Error, "Received Driver message, but unable to inject handler for driver message ", LOGGER);
                }

                return null;
            });

            LOGGER.Log(Level.Info, "task message source injected");
            _currentStatus = new TaskStatus(heartBeatManager, contextId, taskId, messageSources);
        }

        public string TaskId
        {
            get { return _currentStatus.TaskId; }
        }

        public string ContextId
        {
            get { return _currentStatus.ContextId; }
        }

        public void Initialize()
        {
            _currentStatus.SetRunning();
        }

        /// <summary>
        /// Runs the task asynchronously.
        /// </summary>
        public void RunTask()
        {
            LOGGER.Log(Level.Info, "Call Task");
            if (_currentStatus.IsNotRunning())
            {
                var e = new InvalidOperationException("TaskRuntime not in Running state, instead it is in state " + _currentStatus.State);
                Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
            }

            ITask userTask;
            try
            {
                userTask = _injector.GetInstance<ITask>();
            }
            catch (Exception e)
            {
                Utilities.Diagnostics.Exceptions.CaughtAndThrow(new InvalidOperationException("Unable to inject task.", e), Level.Error, "Unable to inject task.", LOGGER);
                return;
            }

            System.Threading.Tasks.Task.Run(() => userTask.Call(null)).ContinueWith(
                runTask =>
                {
                    try
                    {
                        // Task failed.
                        if (runTask.IsFaulted)
                        {
                            LOGGER.Log(Level.Warning,
                                string.Format(CultureInfo.InvariantCulture, "Task failed caused by exception [{0}]", runTask.Exception));
                            _currentStatus.SetException(runTask.Exception);
                            return;
                        }

                        if (runTask.IsCanceled)
                        {
                            LOGGER.Log(Level.Warning,
                                string.Format(CultureInfo.InvariantCulture, "Task failed caused by task cancellation"));
                            return;
                        }

                        // Task completed.
                        var result = runTask.Result;
                        LOGGER.Log(Level.Info, "Task Call Finished");
                        _currentStatus.SetResult(result);
                        if (result != null && result.Length > 0)
                        {
                            LOGGER.Log(Level.Info, "Task running result:\r\n" + System.Text.Encoding.Default.GetString(result));
                        }
                    }
                    finally
                    {
                        userTask.Dispose();
                        runTask.Dispose();
                    }
                });
        }

        public TaskState GetTaskState()
        {
            return _currentStatus.State;
        }

        /// <summary>
        /// Called by heartbeat manager
        /// </summary>
        /// <returns>  current TaskStatusProto </returns>
        public TaskStatusProto GetStatusProto()
        {
            return _currentStatus.ToProto();
        }

        public bool HasEnded()
        {
            return _currentStatus.HasEnded();
        }

        public void Close(byte[] message)
        {
            LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Trying to close Task {0}", TaskId));
            if (_currentStatus.IsNotRunning())
            {
                LOGGER.Log(Level.Warning, string.Format(CultureInfo.InvariantCulture, "Trying to close an task that is in {0} state. Ignored.", _currentStatus.State));
                return;
            }
            try
            {
                OnNext(new CloseEventImpl(message));
                _currentStatus.SetCloseRequested();
            }
            catch (Exception e)
            {
                Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, "Error during Close.", LOGGER);
                _currentStatus.SetException(new TaskClientCodeException(TaskId, ContextId, "Error during Close().", e));
            }
        }

        public void Suspend(byte[] message)
        {
            LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Trying to suspend Task {0}", TaskId));

            if (_currentStatus.IsNotRunning())
            {
                LOGGER.Log(Level.Warning, string.Format(CultureInfo.InvariantCulture, "Trying to supend an task that is in {0} state. Ignored.", _currentStatus.State));
                return;
            }
            try
            {
                OnNext(new SuspendEventImpl(message));
                _currentStatus.SetSuspendRequested();
            }
            catch (Exception e)
            {
                Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, "Error during Suspend.", LOGGER);
                _currentStatus.SetException(
                    new TaskClientCodeException(TaskId, ContextId, "Error during Suspend().", e));
            }
        }

        public void Deliver(byte[] message)
        {
            if (_currentStatus.IsNotRunning())
            {
                LOGGER.Log(Level.Warning, string.Format(CultureInfo.InvariantCulture, "Trying to send a message to an task that is in {0} state. Ignored.", _currentStatus.State));
                return;
            }
            try
            {
                OnNext(new DriverMessageImpl(message));
            }
            catch (Exception e)
            {
                Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, "Error during message delivery.", LOGGER);
                _currentStatus.SetException(
                    new TaskClientCodeException(TaskId, ContextId, "Error during message delivery.", e));
            }
        }

        public void OnNext(ICloseEvent value)
        {
            LOGGER.Log(Level.Info, "TaskRuntime::OnNext(ICloseEvent value)");
            // TODO: send a heartbeat
        }

        public void OnNext(ISuspendEvent value)
        {
            LOGGER.Log(Level.Info, "TaskRuntime::OnNext(ISuspendEvent value)");
            // TODO: send a heartbeat
        }

        public void OnNext(IDriverMessage value)
        {
            LOGGER.Log(Level.Info, "TaskRuntime::OnNext(IDriverMessage value)");

            if (_driverMessageHandler.Value == null)
            {
                return;
            }
            try
            {
                _driverMessageHandler.Value.Handle(value);
            }
            catch (Exception e)
            {
                Utilities.Diagnostics.Exceptions.Caught(e, Level.Warning, "Exception throw when handling driver message: " + e, LOGGER);
                _currentStatus.RecordExecptionWithoutHeartbeat(e);
            }
        }

        /// <summary>
        /// Propagates the IDriverConnection message to the Handler as specified by the Task.
        /// </summary>
        internal void HandleDriverConnectionMessage(IDriverConnectionMessage message)
        {
            if (_driverConnectionMessageHandler.Value == null)
            {
                return;
            }

            _driverConnectionMessageHandler.Value.OnNext(message);
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }
    }
}