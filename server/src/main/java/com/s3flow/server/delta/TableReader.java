package com.s3flow.server.delta;

import io.delta.kernel.*;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.FileStatus;
import io.vertx.core.shareddata.Shareable;
import io.vertx.rxjava3.core.Vertx;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import io.delta.kernel.utils.CloseableIterator;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.task.*;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.zookeeper.data.Stat;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;

@Slf4j
public class TableReader implements Shareable {
  private final Vertx vertx;
  private final HelixManager manager;
  private final TaskDriver taskDriver;
  private final JobQueue jobQueue;
  private final String tableName;
  private final Engine engine;
  private final Table table;
  private final String path;
  private final String wrokflowId;
  private Long startVersion;
  private Long latestVersion;

  public TableReader(
      Vertx vertx,
      HelixManager manager,
      Configuration hadoopConf,
      String tableName,
      String path,
      Long startVersion) {
    this.vertx = vertx;
    this.manager = manager;
    this.taskDriver = new TaskDriver(manager);
    this.tableName = tableName;
    WorkflowConfig.Builder workflowCfgBuilder =
        new WorkflowConfig.Builder()
            .setWorkFlowType("delta-reader")
            .setAllowOverlapJobAssignment(true);
    this.jobQueue =
        new JobQueue.Builder("reader-queue")
                .setWorkflowConfig(workflowCfgBuilder.build()).build();

    this.wrokflowId = jobQueue.getWorkflowConfig().getWorkflowId();
    try {
      taskDriver.start(jobQueue);
    } catch (Exception e) {
      log.error("e");
    }

    this.engine = DefaultEngine.create(hadoopConf);
    this.table = Table.forPath(engine, path);
    this.path = path;
    this.startVersion = startVersion;
  }

  public void start() {
    vertx.setPeriodic(
        5_000,
        l -> {
          taskDriver.cleanupQueue("reader-queue");
          latestVersion = table.getLatestSnapshot(engine).getVersion(engine);
          long currentVersion = getCurrentVersion();
          try {
            if (currentVersion < latestVersion) {
              processCurrentSnapshot(currentVersion);
              incrementCurrentVersion();
            }
          } catch (Exception e) {
            log.error(
                String.format(
                    "error reading snapshot version %s for table %s", currentVersion, path));
          }
        });
  }

  private Long getCurrentVersion() {
    if (!manager
        .getHelixPropertyStore()
        .exists(String.format("/METADATA_ROOT_PATH/%s", tableName), AccessOption.PERSISTENT)) {
      ZNRecord record = new ZNRecord("CURRENT_VERSION");
      record.setLongField("version", startVersion);
      manager
          .getHelixPropertyStore()
          .create(
              String.format("/METADATA_ROOT_PATH/%s", tableName), record, AccessOption.PERSISTENT);
      return startVersion;
    }
    Stat stat = new Stat();
    return manager
        .getHelixPropertyStore()
        .get(String.format("/METADATA_ROOT_PATH/%s", tableName), stat, AccessOption.PERSISTENT)
        .getLongField("version", startVersion);
  }

  private void incrementCurrentVersion() {
    manager
        .getHelixPropertyStore()
        .update(
            String.format("/METADATA_ROOT_PATH/%s", tableName),
            record -> {
              long currentVersion = record.getLongField("version", startVersion);
              currentVersion++;
              record.setLongField("version", currentVersion);
              return record;
            },
            AccessOption.PERSISTENT);
  }

  private long processCurrentSnapshot(long currentVersion) {
    Snapshot snapshot = table.getSnapshotAsOfVersion(engine, currentVersion);
    Scan scan = snapshot.getScanBuilder(engine).build();

    Row scanState = scan.getScanState(engine);
    CloseableIterator<FilteredColumnarBatch> fileIter = scan.getScanFiles(engine);

    JobConfig.Builder jobCfgBuilder = new JobConfig.Builder();
    Map<String, TaskConfig> taskCfgMap = new HashMap<>();
    String jobName = String.format("%s-v%s-reader", tableName, currentVersion);
    List<TaskConfig> taskCfgs = new ArrayList<>();
    int i = 0;
    while (fileIter.hasNext()) {
      try (CloseableIterator<Row> scanFileRows = fileIter.next().getRows()) {
        while (scanFileRows.hasNext()) {
          ScanFile scanFile = new ScanFile(scanState, scanFileRows.next());
          Map<String, String> config = new HashMap<>();
          config.put("state", scanFile.getStateJson());
          config.put("file", scanFile.getFileJson());

          TaskConfig taskCfg = new TaskConfig("READ", config, "" + i++, null);
          taskCfgs.add(taskCfg);
          taskCfgMap.put(taskCfg.getId(), taskCfg);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    jobCfgBuilder.addTaskConfigs(taskCfgs);
    jobCfgBuilder.addTaskConfigMap(taskCfgMap);
    jobCfgBuilder.setNumberOfTasks(taskCfgs.size());
    jobCfgBuilder.setNumConcurrentTasksPerInstance(taskCfgs.size());
    jobCfgBuilder.setJobId(jobName);
    jobCfgBuilder.setWorkflow(wrokflowId);
    taskDriver.enqueueJob("reader-queue", jobName, jobCfgBuilder);
    return currentVersion;
  }

  @SneakyThrows
  public void processBatch(String state, String file) {
    ScanFile work = new ScanFile(state, file);
    Row scanState = work.getScanRow();
    Row scanFile = work.getScanFileRow();
    FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFile);
    StructType physicalReadSchema = ScanStateRow.getPhysicalDataReadSchema(engine, scanState);

    CloseableIterator<ColumnarBatch> physicalDataIter =
        engine
            .getParquetHandler()
            .readParquetFiles(
                singletonCloseableIterator(fileStatus), physicalReadSchema, Optional.empty());

    try (CloseableIterator<FilteredColumnarBatch> dataIter =
            Scan.transformPhysicalData(engine, scanState, scanFile, physicalDataIter);
//        BufferedWriter writer =
//            new BufferedWriter(
//                new FileWriter(String.format("target/output-%s.txt", UUID.randomUUID())))
    ) {
      while (dataIter.hasNext()) {
        FilteredColumnarBatch batch = dataIter.next();
        CloseableIterator<Row> rows = batch.getRows();
        while (rows.hasNext()) {
          Row row = rows.next();
//          writer.append(row.getString(0)).append(" : ").append(row.getString(1));
//          writer.newLine();
          System.out.println(row.getString(0) + " : " + row.getString(1));
        }
      }
    } catch (Exception e) {
    }
  }
}
