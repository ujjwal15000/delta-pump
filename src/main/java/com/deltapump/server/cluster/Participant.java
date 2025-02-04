package com.deltapump.server.cluster;

import com.deltapump.server.config.HelixConfig;
import io.vertx.rxjava3.core.Vertx;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.OnlineOfflineSMD;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.TaskDriver;


public class Participant {
  private final Vertx vertx;
  private final HelixConfig config;
  private final HelixManager manager;
  private TaskDriver taskDriver;

  public Participant(Vertx vertx, HelixConfig config) {
    this.vertx = vertx;
    this.config = config;
    this.manager =
        HelixManagerFactory.getZKHelixManager(
            config.getClusterName(),
            config.getInstanceName(),
            InstanceType.PARTICIPANT,
            config.getZhHost());
  }

  public void connect() throws Exception {
    int parallelism = Integer.parseInt(System.getProperty("worker.parallelism", "1"));
    StateMachineEngine stateMach = manager.getStateMachineEngine();
    stateMach.registerStateModelFactory(
        OnlineOfflineSMD.name,
        new WorkerStateModelFactory(vertx, config.getInstanceName(), parallelism));
    manager.connect();
    this.manager.connect();
    taskDriver = new TaskDriver(manager);
  }
}
