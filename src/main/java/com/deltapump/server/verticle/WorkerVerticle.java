package com.deltapump.server.verticle;

import com.deltapump.server.FlowServer;
import com.deltapump.server.cluster.Participant;
import com.deltapump.server.cluster.ZKAdmin;
import com.deltapump.server.config.HelixConfig;
import io.reactivex.rxjava3.core.Completable;
import io.vertx.rxjava3.core.AbstractVerticle;
import lombok.SneakyThrows;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Objects;

public class WorkerVerticle extends AbstractVerticle {
    private Participant participant;
    private ZKAdmin zkAdmin;
    private HelixConfig config;

    @SneakyThrows
    @Override
    public Completable rxStart(){
        String zkHost = System.getProperty("zkHost", null);
        assert !Objects.equals(zkHost, null);

        int port;
        try(ServerSocket socket = new ServerSocket(0)){
            port = socket.getLocalPort();
        }

        InetAddress localHost = InetAddress.getLocalHost();
        String nodeId = localHost.getHostAddress() + "_" + port;
        this.zkAdmin = (ZKAdmin) vertx.sharedData().getLocalMap(FlowServer.SHARED_MAP).get(ZKAdmin.class.getName());
        zkAdmin.addNode(nodeId);

        this.config = new HelixConfig(zkHost, "s3-flow-cluster", nodeId);
        this.participant = new Participant(vertx, config);
        participant.connect();

        return Completable.complete();
    }

    @Override
    public Completable rxStop(){
        return Completable.complete();
    }
}
