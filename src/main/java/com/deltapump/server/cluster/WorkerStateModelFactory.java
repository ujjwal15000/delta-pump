package com.deltapump.server.cluster;

import com.deltapump.server.PumpServer;
import com.deltapump.server.deltareader.TableReader;
import com.deltapump.server.model.TableFileState;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.rxjava3.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;

@Slf4j
public class WorkerStateModelFactory extends StateModelFactory<StateModel> {
  private final String instanceName;
  private final Vertx vertx;

  public WorkerStateModelFactory(
      Vertx vertx, String instanceName) {
    this.instanceName = instanceName;
    this.vertx = vertx;
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    log.debug(
        "Creating new StateModel for resource: {} and partition: {}", resourceName, partitionName);
    return new OnlineOfflineStateModel(
        vertx, instanceName, resourceName, partitionName);
  }

  public static class OnlineOfflineStateModel extends StateModel {
    private final String instanceName;
    private final String partitionName;
    private final String resourceName;
    private final Vertx vertx;
    private final TableReader tableReader;
    private final Integer partitionId;
    private Long filePoller;

    public OnlineOfflineStateModel(
        Vertx vertx,
        String instanceName,
        String resourceName,
        String partitionName) {
      super();
      this.instanceName = instanceName;
      this.resourceName = resourceName;
      this.partitionName = partitionName;
      this.vertx = vertx;
      log.info(
          "Initialized OnlineOfflineStateModel for instance: {}, resource: {}, partition: {}",
          instanceName,
          resourceName,
          partitionName);
      this.tableReader =
              (TableReader) vertx.sharedData().getLocalMap(PumpServer.SHARED_MAP).get(TableReader.class.getName());
      this.partitionId = Integer.parseInt(partitionName.split("_")[partitionName.split("_").length - 1]);
    }

    public void onBecomeOnlineFromOffline(Message message, NotificationContext context)
        throws Exception {
      log.info(
          "Transitioning from OFFLINE to ONLINE for resource: {}, partition: {}",
          resourceName,
          partitionName);
      this.startWorker();
    }

    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      log.info(
          "Transitioning from ONLINE to OFFLINE for resource: {}, partition: {}",
          resourceName,
          partitionName);
      if (filePoller != null) {
        vertx.cancelTimer(filePoller);
        filePoller = null;
      }
    }

    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      log.warn(
          "Transitioning from OFFLINE to DROPPED for resource: {}, partition: {}",
          resourceName,
          partitionName);
      // Add cleanup logic here if necessary.
    }

    private void startWorker(){
      filePoller = vertx.setPeriodic(5_000, l -> {
        Observable.fromIterable(tableReader.getFilesForCurrentPartition(partitionId))
                .concatMapCompletable(r -> {
                  try{
                    tableReader.updateFileState(r.getFileId(), TableFileState.State.RUNNING);
                    tableReader.processBatch(r.getScanFile().getStateJson(), r.getScanFile().getFileJson());
                    tableReader.updateFileState(r.getFileId(), TableFileState.State.COMPLETED);
                  }
                  catch (Exception e){
                    log.error("error reading file");
                    tableReader.updateFileState(r.getFileId(), TableFileState.State.FAILED);
                  }
                  return Completable.complete();
                }).subscribe();
      });
    }
  }
}
