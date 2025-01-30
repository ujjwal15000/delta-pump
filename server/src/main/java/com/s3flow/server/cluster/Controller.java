package com.s3flow.server.cluster;

import com.s3flow.server.config.HelixConfig;
import io.vertx.rxjava3.core.Vertx;
import lombok.Getter;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.controller.GenericHelixController;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.task.*;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Controller {
  private final HelixConfig config;
  @Getter private final HelixManager manager;
  private final Vertx vertx;

  private TaskDriver taskDriver;

  public Controller(Vertx vertx, HelixConfig config) {
    this.vertx = vertx;
    this.config = config;
    this.manager =
        HelixManagerFactory.getZKHelixManager(
            config.getClusterName(),
            config.getInstanceName(),
            InstanceType.CONTROLLER,
            config.getZhHost());
  }

  public void connect() throws Exception {
    this.manager.connect();
    GenericHelixController controller = new GenericHelixController();
    manager.addControllerListener(controller);
    manager.addInstanceConfigChangeListener(controller);
    manager.addResourceConfigChangeListener(controller);
    manager.addClusterfigChangeListener(controller);
    manager.addCustomizedStateConfigChangeListener(controller);
    manager.addLiveInstanceChangeListener(controller);
    manager.addIdealStateChangeListener(controller);
  }

  public ZkHelixPropertyStore<ZNRecord> getPropertyStore() {
    return manager.getHelixPropertyStore();
  }
}
