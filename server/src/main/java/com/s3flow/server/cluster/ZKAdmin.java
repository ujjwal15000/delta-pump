package com.s3flow.server.cluster;

import com.s3flow.server.config.HelixConfig;
import io.vertx.core.shareddata.Shareable;
import io.vertx.rxjava3.core.Vertx;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.*;

public class ZKAdmin implements Shareable {
  private final HelixConfig config;
  private final ZKHelixAdmin zkHelixAdmin;
  private final Vertx vertx;

  public ZKAdmin(Vertx vertx, HelixConfig config){
    this.vertx = vertx;
    this.config = config;
    this.zkHelixAdmin = new ZKHelixAdmin(config.getZhHost());

    this.initCluster();
  }

  public void initCluster() {
    zkHelixAdmin.addCluster(config.getClusterName());
    zkHelixAdmin.addStateModelDef(
        config.getClusterName(), OnlineOfflineSMD.name, OnlineOfflineSMD.build());
    zkHelixAdmin.addStateModelDef(
        config.getClusterName(), MasterSlaveSMD.name, MasterSlaveSMD.build());
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
    ClusterConfig clusterConfig = dataAccessor.getProperty(dataAccessor.keyBuilder().clusterConfig());
    clusterConfig.setOfflineDurationForPurge(5 * 60 * 1_000);
    dataAccessor.setProperty(dataAccessor.keyBuilder().clusterConfig(), clusterConfig);
  }
}
