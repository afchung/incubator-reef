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
  public void aggregationCompleted(final List<Integer> taskletIds, final byte[] serializedResult) {
  }

  @Override
  public void threwException(final int taskletId, final Exception exception) {
  }

  @Override
  public void aggregationThrewException(final List<Integer> taskletIds, final Exception exception) {
  }

  @Override
  public void cancelled(final int taskletId) {
  }
}
