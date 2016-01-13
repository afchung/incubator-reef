package org.apache.reef.vortex.api;

import org.apache.reef.io.serialization.Codec;

import java.util.List;

/**
 * Created by anchung on 1/13/2016.
 */
public interface VortexAggregateFunction<TInput, TOutput> {

  TOutput call(final List<TInput> input) throws Exception;

  Codec<TOutput> getOutputCodec();
}
