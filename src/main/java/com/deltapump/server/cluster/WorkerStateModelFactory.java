package com.deltapump.server.cluster;

import com.deltapump.server.PumpServer;
import com.deltapump.server.deltareader.TableReader;
import com.deltapump.server.model.TableFileState;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.vertx.rxjava3.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class WorkerStateModelFactory extends StateModelFactory<StateModel> {
  private final String instanceName;
  private final Vertx vertx;
  private final Integer parallelism;

  public WorkerStateModelFactory(Vertx vertx, String instanceName, Integer parallelism) {
    this.instanceName = instanceName;
    this.vertx = vertx;
    this.parallelism = parallelism;
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    log.debug(
        "Creating new StateModel for resource: {} and partition: {}", resourceName, partitionName);
    return new OnlineOfflineStateModel(
        vertx, instanceName, resourceName, partitionName, parallelism);
  }

  public static class OnlineOfflineStateModel extends StateModel {
    private final String instanceName;
    private final String partitionName;
    private final String resourceName;
    private final Vertx vertx;
    private final TableReader tableReader;
    private final Integer partitionId;
    private final Integer parallelism;
    private Long filePoller;

    public OnlineOfflineStateModel(
        Vertx vertx,
        String instanceName,
        String resourceName,
        String partitionName,
        Integer parallelism) {
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
          (TableReader)
              vertx
                  .sharedData()
                  .getLocalMap(PumpServer.SHARED_MAP)
                  .get(TableReader.class.getName());
      this.partitionId =
          Integer.parseInt(partitionName.split("_")[partitionName.split("_").length - 1]);
      this.parallelism = parallelism;
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

    private void startWorker() {
      AtomicInteger running = new AtomicInteger(0);
      filePoller =
          vertx.setPeriodic(
              5_000,
              l -> {
                if (running.compareAndSet(0, 1)) {
                  Flowable.fromIterable(tableReader.getFilesForCurrentPartition(partitionId))
                      .parallel(parallelism)
                      .concatMap(
                          r ->
                              Flowable.fromCompletable(
                                  tableReader
                                      .rxUpdateFileState(
                                          r.getFileId(), TableFileState.State.RUNNING)
                                      .andThen(
                                          tableReader.rxProcessBatch(
                                              r.getScanFile().getStateJson(),
                                              r.getScanFile().getFileJson()))
                                      .flatMapCompletable(
                                          row -> {
                                            System.out.println(
                                                row.getString(0) + " : " + row.getString(1));
                                            return Completable.complete();
                                          })
                                      .andThen(
                                          tableReader.rxUpdateFileState(
                                              r.getFileId(), TableFileState.State.COMPLETED))))
                      .sequential()
                      .doFinally(() -> running.set(0))
                      .subscribe();
                }
              });
    }
  }
}
