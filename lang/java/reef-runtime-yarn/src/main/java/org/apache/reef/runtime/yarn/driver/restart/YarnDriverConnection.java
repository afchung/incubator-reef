package org.apache.reef.runtime.yarn.driver.restart;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.reef.runtime.common.evaluator.DriverConnection;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by anchung on 11/3/2015.
 */
public class YarnDriverConnection implements DriverConnection {
  private final CloseableHttpClient client;
  private final InetSocketAddress rmAddress;
  private final HttpGet getRequest;

  @Inject
  private YarnDriverConnection(final YarnConfiguration configuration, final String applicationId) {
    this.rmAddress = configuration.getSocketAddr(YarnConfiguration.RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_PORT);

    this.client = HttpClients.createDefault();

    // Example, for HDInsight: http://headnodehost:9014/proxy/application_1407519727821_0012/reef/v1/driver
    this.getRequest = new HttpGet("http://" + rmAddress.getHostName() + ":" + rmAddress.getPort() + "/proxy/" +
        applicationId + "/reef/v1/driver");
  }
  @Override
  public String getDriverRemoteIdentifier() throws IOException {
    final CloseableHttpResponse response = client.execute(getRequest, new HttpClientContext());

    // TODO: parse response headers and body
    return null;
  }

  @Override
  public void close() throws Exception {
    client.close();
  }
}
