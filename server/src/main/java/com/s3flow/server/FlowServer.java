package com.s3flow.server;

import com.s3flow.server.cluster.Controller;
import com.s3flow.server.cluster.ZKAdmin;
import com.s3flow.server.config.HelixConfig;
import com.s3flow.server.verticle.WorkerVerticle;
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

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
public class FlowServer {
  public static final String WORKER_POOL_NAME = "default-worker-pool";
  public static final String SHARED_MAP = "shared-map";

  private final Vertx vertx;
  private final HelixConfig config;
  private final ZKAdmin zkAdmin;
  private final Controller controller;

  private final Thread shutdownHook = new Thread(() -> this.stop(30_000));

  public static void main(String[] args) throws Exception {
    FlowServer server = new FlowServer();
    server.start();
  }

  public FlowServer() throws Exception {

    String zkHost = System.getProperty("zkHost", null);
    assert !Objects.equals(zkHost, null);

    String nodeId = UUID.randomUUID().toString();
    this.vertx = initVertx();
    this.config = new HelixConfig(zkHost, "s3-flow-cluster", nodeId);

    this.zkAdmin = new ZKAdmin(vertx, config);
    vertx.sharedData().getLocalMap(SHARED_MAP).put(ZKAdmin.class.getName(), this.zkAdmin);

    this.controller = new Controller(vertx, config);
    controller.connect();

    if (controller.getManager().isLeader())
      ZKAdmin.addClusterConfigs(controller.getManager());
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
                .setInstances(CpuCoreSensor.availableProcessors())
                .setWorkerPoolName(WORKER_POOL_NAME))
        .ignoreElement();
  }

  private void stop(int timeout) {
    Completable.complete()
        .delay(timeout, TimeUnit.MILLISECONDS)
        .andThen(vertx.rxClose())
        .subscribe(
            () -> log.info("successfully stopped server"),
            (e) -> log.error("error stopping server: ", e));
  }
}
