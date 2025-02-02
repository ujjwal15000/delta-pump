package com.deltapump.server;

import com.deltapump.server.cluster.Controller;
import com.deltapump.server.cluster.ZKAdmin;
import com.deltapump.server.config.HelixConfig;
import com.deltapump.server.deltareader.TableReader;
import com.deltapump.server.verticle.WorkerVerticle;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import io.vertx.rxjava3.core.RxHelper;
import io.vertx.rxjava3.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.api.listeners.ResourceConfigChangeListener;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.ResourceConfig;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
public class PumpServer {
  public static final String WORKER_POOL_NAME = "default-worker-pool";
  public static final String SHARED_MAP = "shared-map";

  private final Vertx vertx;
  private final HelixConfig config;
  private final ZKAdmin zkAdmin;
  private final Controller controller;
  private final TableReader tableReader;

  private final Thread shutdownHook = new Thread(() -> this.stop(30_000));

  public static void main(String[] args) throws Exception {
    PumpServer server = new PumpServer();
    server.start();
  }

  public PumpServer() throws Exception {
    String zkHost = System.getProperty("zkHost", null);
    Objects.requireNonNull(zkHost);

    String nodeId = UUID.randomUUID().toString();
    this.vertx = initVertx();
    this.config = new HelixConfig(zkHost, "DELTA_PUMP_CLUSTER", nodeId);

    this.zkAdmin = new ZKAdmin(vertx, config);
    vertx.sharedData().getLocalMap(SHARED_MAP).put(ZKAdmin.class.getName(), this.zkAdmin);

    this.controller = new Controller(vertx, config);
    this.controller.connect();

    this.controller
        .getManager()
        .addLiveInstanceChangeListener(
            (LiveInstanceChangeListener)
                (liveInstances, changeContext) -> {
                  if (this.controller.getManager().isLeader()) zkAdmin.rebalanceWorkerGroup();
                });

    String tablePath = System.getProperty("deltaTable.path");
    Objects.requireNonNull(tablePath);

    Configuration conf = new Configuration();
    this.tableReader =
        new TableReader(
            vertx,
            controller.getManager(),
            conf,
            "t1",
            tablePath,
            0L,
            zkAdmin.getWorkerGroupSize());

    this.controller
        .getManager()
        .addResourceConfigChangeListener(
            (resourceConfigs, context) -> {
              for (ResourceConfig resourceConfig : resourceConfigs) {
                if (Objects.equals(resourceConfig.getResourceName(), "DELTA_PUMP_WORKER_GROUP")) {
                  tableReader.updateWorkers(resourceConfig.getNumPartitions());
                }
              }
            });
    vertx.sharedData().getLocalMap(SHARED_MAP).put(TableReader.class.getName(), tableReader);
    if (controller.getManager().isLeader()) {
      ZKAdmin.addClusterConfigs(controller.getManager());
      tableReader.start();
    }
  }

  private Vertx initVertx() {
    Vertx vertx =
        Vertx.vertx(
            new VertxOptions()
                .setMetricsOptions(
                    new MicrometerMetricsOptions()
                        .setPrometheusOptions(
                            new VertxPrometheusOptions()
                                .setEnabled(true)
                                .setStartEmbeddedServer(true)
                                .setEmbeddedServerOptions(new HttpServerOptions().setPort(9090))
                                .setEmbeddedServerEndpoint("/metrics")))
                .setEventLoopPoolSize(CpuCoreSensor.availableProcessors())
                .setPreferNativeTransport(true));

    RxJavaPlugins.setComputationSchedulerHandler(s -> RxHelper.scheduler(vertx));
    RxJavaPlugins.setIoSchedulerHandler(s -> RxHelper.scheduler(vertx));
    RxJavaPlugins.setNewThreadSchedulerHandler(s -> RxHelper.scheduler(vertx));
    Runtime.getRuntime().addShutdownHook(shutdownHook);
    return vertx;
  }

  private void start() {
    deploySocketVerticle()
        .subscribe(
            () -> log.info("successfully started server"),
            (e) -> log.error("application startup failed: ", e));
  }

  private Completable deploySocketVerticle() {
    return vertx
        .rxDeployVerticle(
            WorkerVerticle::new,
            new DeploymentOptions()
                //                .setInstances(1)
                .setInstances(CpuCoreSensor.availableProcessors())
                .setWorkerPoolName(WORKER_POOL_NAME))
        .ignoreElement();
  }

  private void stop(int delay) {
    Completable.complete()
        .delay(delay, TimeUnit.MILLISECONDS)
        .andThen(vertx.rxClose())
        .subscribe(
            () -> log.info("successfully stopped server"),
            (e) -> log.error("error stopping server: ", e));
  }
}
