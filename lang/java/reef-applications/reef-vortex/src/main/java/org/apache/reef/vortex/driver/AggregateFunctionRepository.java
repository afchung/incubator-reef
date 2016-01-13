package org.apache.reef.vortex.driver;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.vortex.api.VortexAggregateFunction;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by anchung on 1/13/2016.
 */
@ThreadSafe
@Unstable
final class AggregateFunctionRepository {
  private final Map<Integer, VortexAggregateFunction> aggregateFunctionIdMap = new ConcurrentHashMap<>();

  @Inject
  private AggregateFunctionRepository() {
  }

  VortexAggregateFunction put(final int aggregateFunctionId, VortexAggregateFunction function) {
    return aggregateFunctionIdMap.put(aggregateFunctionId, function);
  }

  VortexAggregateFunction get(final int aggregateFunctionId) {
    return aggregateFunctionIdMap.get(aggregateFunctionId);
  }
}
