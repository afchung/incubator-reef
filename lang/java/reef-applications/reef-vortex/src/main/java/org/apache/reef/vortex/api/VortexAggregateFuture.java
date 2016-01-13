/*
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
package org.apache.reef.vortex.api;

import org.apache.reef.io.serialization.Codec;
import org.apache.reef.vortex.common.VortexFutureDelegate;
import org.apache.reef.vortex.driver.VortexMaster;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.logging.Logger;

/**
 * Created by anchung on 1/13/2016.
 */
public class VortexAggregateFuture<TOutput> implements VortexFutureDelegate {
  private static final Logger LOG = Logger.getLogger(VortexFuture.class.getName());

  private final Executor executor;
  private final VortexMaster vortexMaster;
  private final Codec<TOutput> outputCodec;
  private final Set<Integer> taskletIds;

  public VortexAggregateFuture(final Executor executor, final VortexMaster vortexMaster,
                               final Collection<Integer> taskletIds, final Codec<TOutput> outputCodec) {
    this.executor = executor;
    this.vortexMaster = vortexMaster;
    this.taskletIds = new HashSet<>(taskletIds);
    this.outputCodec = outputCodec;
  }

  @Override
  public void completed(final int taskletId, final byte[] serializedResult) {
  }

  @Override
  public void aggregationCompleted(final List<Integer> pTaskletIds, final byte[] serializedResult) {
  }

  @Override
  public void threwException(final int taskletId, final Exception exception) {
  }

  @Override
  public void aggregationThrewException(final List<Integer> pTaskletIds, final Exception exception) {
  }

  @Override
  public void cancelled(final int taskletId) {
  }
}
