package com.s3flow.server.config;

import lombok.Data;

@Data
public class HelixConfig {
    private String zhHost;
    private String clusterName;
    private String instanceName;

    public HelixConfig(String zhHost, String clusterName, String instanceName){
        this.zhHost = zhHost;
        this.clusterName = clusterName;
        this.instanceName = instanceName;
    }
}
