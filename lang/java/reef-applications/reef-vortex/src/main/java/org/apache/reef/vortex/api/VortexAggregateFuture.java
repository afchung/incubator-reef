package org.apache.reef.vortex.api;

import org.apache.reef.vortex.common.VortexFutureDelegate;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by anchung on 1/13/2016.
 */
public class VortexAggregateFuture<TOutput> implements VortexFutureDelegate {
  private final Set<Integer> taskletIds = new HashSet<>();

  public VortexAggregateFuture() {
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
