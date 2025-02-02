package com.deltapump.server.deltareader;

import com.deltapump.server.model.TableFileState;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.zookeeper.data.Stat;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;

@Slf4j
public class TableReader implements Shareable {
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final Vertx vertx;
  private final HelixManager manager;
  private final String tableName;
  private final Engine engine;
  private final Table table;
  private final String path;
  private Long startVersion;
  private Long latestVersion;
  private boolean processing = false;

  public TableReader(
      Vertx vertx,
      HelixManager manager,
      Configuration hadoopConf,
      String tableName,
      String path,
      Long startVersion) {
    this.vertx = vertx;
    this.manager = manager;
    this.tableName = tableName;
    this.engine = DefaultEngine.create(hadoopConf);
    this.table = Table.forPath(engine, path);
    this.path = path;
    this.startVersion = startVersion;
  }

  public void start() {
    vertx.setPeriodic(
        5_000,
        l -> {
          long currentVersion = getCurrentVersion();
          try {
            if (!processing && currentVersion < latestVersion) {
              latestVersion = table.getLatestSnapshot(engine).getVersion(engine);
              processCurrentSnapshot(currentVersion);
              processing = true;
            } else {
              List<TableFileState> map = getCurrentStateMap();
              if (isCurrentVersionDone(map)) {
                incrementCurrentVersion();
                processing = false;
              }
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
        .exists(
            String.format("/METADATA_ROOT_PATH/%s/VERSION", tableName), AccessOption.PERSISTENT)) {
      ZNRecord record = new ZNRecord("CURRENT_VERSION");
      record.setLongField("version", startVersion);
      manager
          .getHelixPropertyStore()
          .create(
              String.format("/METADATA_ROOT_PATH/%s/VERSION", tableName),
              record,
              AccessOption.PERSISTENT);
      return startVersion;
    }
    Stat stat = new Stat();
    return manager
        .getHelixPropertyStore()
        .get(
            String.format("/METADATA_ROOT_PATH/%s/VERSION", tableName),
            stat,
            AccessOption.PERSISTENT)
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

  private void processCurrentSnapshot(long currentVersion) {
    Snapshot snapshot = table.getSnapshotAsOfVersion(engine, currentVersion);
    Scan scan = snapshot.getScanBuilder(engine).build();

    Row scanState = scan.getScanState(engine);
    CloseableIterator<FilteredColumnarBatch> fileIter = scan.getScanFiles(engine);

    List<ScanFile> files = new ArrayList<>();
    while (fileIter.hasNext()) {
      try (CloseableIterator<Row> scanFileRows = fileIter.next().getRows()) {
        while (scanFileRows.hasNext()) {
          ScanFile scanFile = new ScanFile(scanState, scanFileRows.next());
          files.add(scanFile);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    addNewStateMap(files);
  }

  private void addNewStateMap(List<ScanFile> files) {
    List<String> stateMap = initStateMap(files);
    ZNRecord record = new ZNRecord("CURRENT_STATE");

    record.setListField("current-state", stateMap);
    if (!manager
        .getHelixPropertyStore()
        .exists(
            String.format("/METADATA_ROOT_PATH/%s/STATE_MAP", tableName),
            AccessOption.PERSISTENT)) {
      manager
          .getHelixPropertyStore()
          .create(
              String.format("/METADATA_ROOT_PATH/%s/STATE_MAP", tableName),
              record,
              AccessOption.PERSISTENT);
    } else
      manager
          .getHelixPropertyStore()
          .set(
              String.format("/METADATA_ROOT_PATH/%s/STATE_MAP", tableName),
              record,
              AccessOption.PERSISTENT);
  }

  private List<TableFileState> getCurrentStateMap() {
    if (!manager
        .getHelixPropertyStore()
        .exists(
            String.format("/METADATA_ROOT_PATH/%s/STATE_MAP", tableName),
            AccessOption.PERSISTENT)) {
      return new ArrayList<>();
    }
    Stat stat = new Stat();
    return manager
        .getHelixPropertyStore()
        .get(
            String.format("/METADATA_ROOT_PATH/%s/STATE_MAP", tableName),
            stat,
            AccessOption.PERSISTENT)
        .getListField("current-state")
        .stream()
            .map(r -> {
              try {
                return objectMapper.readValue(r, TableFileState.class);
              } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
              }
            })
        .collect(Collectors.toList());
  }

  public List<TableFileState> getFilesForCurrentPartition(Integer partitionId) {
    List<TableFileState> stateMap = getCurrentStateMap();
    List<TableFileState> partitionMap = new ArrayList<>();
    for(int i=0;i<stateMap.size();i++){
      if(i % partitionId == 0)
        partitionMap.add(stateMap.get(i));
    }
    return partitionMap;
  }


  @SneakyThrows
  private List<String> initStateMap(List<ScanFile> files) {
    List<String> map = new ArrayList<>();
    for (int i = 0; i < files.size(); i++) {
      map.add(objectMapper.writeValueAsString(TableFileState.builder().scanFile(files.get(i)).state(TableFileState.State.PENDING).build()));
    }
    return map;
  }

  @SneakyThrows
  public void updateFileState(Integer fileId, TableFileState.State state) {
    Stat stat = new Stat();
    ZNRecord record = manager
            .getHelixPropertyStore()
            .get(
                    String.format("/METADATA_ROOT_PATH/%s/STATE_MAP", tableName),
                    stat,
                    AccessOption.PERSISTENT);
    List<TableFileState> stateMap = record
            .getListField("current-state")
            .stream()
            .map(r -> {
              try {
                return objectMapper.readValue(r, TableFileState.class);
              } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
              }
            })
            .collect(Collectors.toList());
    stateMap.get(fileId).setState(state);
    List<String> updated = stateMap.stream().map(r -> {
        try {
            return objectMapper.writeValueAsString(r);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }).collect(Collectors.toList());
    record.setListField("current-state", updated);
    boolean set = false;
    while(!set){
      set = manager.getHelixPropertyStore()
              .set(String.format("/METADATA_ROOT_PATH/%s/STATE_MAP", tableName),
              record, stat.getVersion(), AccessOption.PERSISTENT);
    }
  }

  private boolean isCurrentVersionDone(List<TableFileState> fileStates){
    for(TableFileState fileState: fileStates){
      if(!fileState.getState().equals(TableFileState.State.COMPLETED))
        return false;
    }
    return true;
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
            BufferedWriter writer =
                new BufferedWriter(
                    new FileWriter(String.format("target/output-%s.txt", UUID.randomUUID())))
    ) {
      while (dataIter.hasNext()) {
        FilteredColumnarBatch batch = dataIter.next();
        CloseableIterator<Row> rows = batch.getRows();
        while (rows.hasNext()) {
          Row row = rows.next();
                    writer.append(row.getString(0)).append(" : ").append(row.getString(1));
                    writer.newLine();
          System.out.println(row.getString(0) + " : " + row.getString(1));
        }
      }
    } catch (Exception e) {
    }
  }
}
