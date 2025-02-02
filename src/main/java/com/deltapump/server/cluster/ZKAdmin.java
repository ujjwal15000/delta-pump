package com.deltapump.server.cluster;

import com.deltapump.server.config.HelixConfig;
import io.vertx.core.shareddata.Shareable;
import io.vertx.rxjava3.core.Vertx;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.*;

public class ZKAdmin implements Shareable {
  private final HelixConfig config;
  private final ZKHelixAdmin zkHelixAdmin;
  private final Vertx vertx;

  public ZKAdmin(Vertx vertx, HelixConfig config) {
    this.vertx = vertx;
    this.config = config;
    this.zkHelixAdmin = new ZKHelixAdmin(config.getZhHost());

    this.initCluster();
    this.initWorkerGroup();
  }

  public void initCluster() {
    zkHelixAdmin.addCluster(config.getClusterName());
    zkHelixAdmin.addStateModelDef(
        config.getClusterName(), OnlineOfflineSMD.name, OnlineOfflineSMD.build());
    zkHelixAdmin.addStateModelDef(
        config.getClusterName(), MasterSlaveSMD.name, MasterSlaveSMD.build());
  }

  public void initWorkerGroup() {
    List<String> resources = zkHelixAdmin.getResourcesInCluster(config.getClusterName());
    if (!resources.contains("DELTA_PUMP_WORKER_GROUP")) {
      IdealState idealState = new IdealState("DELTA_PUMP_WORKER_GROUP");
      idealState.setNumPartitions(8);
      idealState.setStateModelDefRef(OnlineOfflineSMD.name);
      IdealState.RebalanceMode mode =
          idealState.rebalanceModeFromString(
              IdealState.RebalanceMode.FULL_AUTO.name(), IdealState.RebalanceMode.SEMI_AUTO);
      idealState.setRebalanceMode(mode);
      idealState.setReplicas("1");

      zkHelixAdmin.addResource(config.getClusterName(), "DELTA_PUMP_WORKER_GROUP", idealState);
    }
  }

  public Integer getWorkerGroupSize() {
    return zkHelixAdmin
        .getResourceIdealState(config.getClusterName(), "DELTA_PUMP_WORKER_GROUP")
        .getNumPartitions();
  }

  public void rebalanceWorkerGroup() {
    zkHelixAdmin.rebalance(config.getClusterName(), "DELTA_PUMP_WORKER_GROUP", 1);
  }

  public void addNode(String instanceName) throws IOException {
    InstanceConfig instanceConfig = new InstanceConfig(instanceName);
    instanceConfig.setHostName(InetAddress.getLocalHost().getHostAddress());
    instanceConfig.setPort(String.valueOf(new ServerSocket(0).getLocalPort()));
    instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);

    zkHelixAdmin.addInstance(config.getClusterName(), instanceConfig);
  }

  public static void addClusterConfigs(HelixManager manager) {
    HelixDataAccessor dataAccessor = manager.getHelixDataAccessor();
    ClusterConfig clusterConfig =
        dataAccessor.getProperty(dataAccessor.keyBuilder().clusterConfig());
    clusterConfig.setOfflineDurationForPurge(5 * 60 * 1_000);
    dataAccessor.setProperty(dataAccessor.keyBuilder().clusterConfig(), clusterConfig);
  }
}
