package org.apache.reef.infra.watchdog;

import org.apache.http.HttpHost;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.reef.infra.watchdog.example.HelloDriver;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.handler.ContextHandler;

import javax.inject.Inject;
import java.net.InetAddress;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by anchung on 3/28/2016.
 */
public final class Launcher implements AutoCloseable {
  private static final int PORT = 8080;
  private static final int MAX_NUM_EVALUATORS_PER_JOB = 10;
  private static final Logger LOG = Logger.getLogger(Launcher.class.getName());

  private static CountDownLatch latch = new CountDownLatch(1);
  private final ContextHandler watchdogRequestHandler;
  private final Server server;

  @Inject
  private Launcher(final WatchdogRequestHandler requestHandler) {
    this.server = new Server();
    final SocketConnector connector = new SocketConnector();
    connector.setHost(InetAddress.getLoopbackAddress().getHostAddress());
    connector.setPort(PORT);
    server.addConnector(connector);
    this.watchdogRequestHandler = new ContextHandler("/submit");
    this.watchdogRequestHandler.setHandler(requestHandler);
  }

  void start() throws Exception {
    server.setHandler(watchdogRequestHandler);
    server.start();
  }

  public static void main(final String[] args) throws InterruptedException {
    // TODO: Get num evaluators per process.
    // TODO: Get Host/Port.

    final Thread serverThread = new Thread(new Runnable() {
      @Override
      public void run() {
        runServer();
      }
    });

    serverThread.start();

    final Thread requestThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          LOG.log(Level.SEVERE, "HELLO1");
          latch.await();
          final HttpClient client = HttpClients.createDefault();
          final HttpHost host = new HttpHost(InetAddress.getLoopbackAddress().getHostAddress(), PORT);
          final HttpPost request = new HttpPost("/submit/");
          final String configStr = Tang.Factory.getTang().newInjector().getInstance(ConfigurationSerializer.class)
              .toString(HelloDriver.getDriverConfiguration());
          request.setEntity(new ByteArrayEntity(configStr.getBytes("UTF8")));
          client.execute(host, request);
        } catch (final Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    });

    requestThread.start();
    requestThread.join();

    serverThread.join();
  }

  private static void runServer() {
    final Injector injector = Tang.Factory.getTang().newInjector(
        SubmissionRequestHandlerConfiguration.CONF
            .set(SubmissionRequestHandlerConfiguration.MAX_NUM_EVALUATORS_PER_JOB, MAX_NUM_EVALUATORS_PER_JOB)
            .set(SubmissionRequestHandlerConfiguration.WATCHDOG_ADDRESS,
                InetAddress.getLoopbackAddress().getHostAddress())
            .set(SubmissionRequestHandlerConfiguration.WATCHDOG_PORT, PORT)
            .build());
    try (final Launcher launcher = injector.getInstance(Launcher.class)){
      launcher.start();
      LOG.log(Level.SEVERE, "HELLO2");
      latch.countDown();
      launcher.join();
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void join() throws InterruptedException {
    server.join();
  }

  @Override
  public void close() throws Exception {
    server.stop();
  }
}
