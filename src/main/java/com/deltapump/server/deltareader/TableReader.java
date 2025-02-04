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
import io.delta.kernel.expressions.*;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.TableImpl;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.FileStatus;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.shareddata.Shareable;
import io.vertx.rxjava3.core.Vertx;
import lombok.Data;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;

@Data
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
  private final AtomicInteger numWorkers = new AtomicInteger();

  public TableReader(
      Vertx vertx,
      HelixManager manager,
      Configuration hadoopConf,
      String tableName,
      String path,
      Long startVersion,
      Integer numWorkers) {
    this.vertx = vertx;
    this.manager = manager;
    this.tableName = tableName;
    this.engine = DefaultEngine.create(hadoopConf);
    this.table = TableImpl.forPath(engine, path);
    this.path = path;
    this.startVersion = startVersion;
    this.numWorkers.set(numWorkers);
  }

  public void start() {
    vertx.setPeriodic(
        5_000,
        l -> {
          long currentVersion = getCurrentVersion();
          latestVersion = table.getLatestSnapshot(engine).getVersion(engine);
          try {
            if (currentVersion <= latestVersion) {
              if (!processing) {
                processCurrentSnapshot(currentVersion);
                processing = true;
              } else {
                List<TableFileState> map = getCurrentStateMap();
                if (isCurrentVersionDone(map)) {
                  incrementCurrentVersion();
                  processing = false;
                }
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
            String.format("/METADATA_ROOT_PATH/%s/VERSION", tableName),
            record -> {
              long currentVersion = record.getLongField("version", startVersion);
              currentVersion++;
              record.setLongField("version", currentVersion);
              return record;
            },
            AccessOption.PERSISTENT);
  }

  private void processCurrentSnapshot(long currentVersion) {
    List<ScanFile> scanFiles = getScanFiles(table, engine, currentVersion);
    addNewStateMap(scanFiles);
  }

  public static void main(String[] args) {
    Configuration conf = new Configuration();
    Engine engine = DefaultEngine.create(conf);
    Table table = TableImpl.forPath(engine, "/Users/ujjwalbagrania/Desktop/notebooks/delta");

    List<String> v10 = getScannedFiles(table, engine, 9);
    //    List<String> v10 = new ArrayList<>();
    List<String> v11 = getScannedFiles(table, engine, 10);
    v11.removeAll(v10);

    Snapshot currSnapshot = table.getSnapshotAsOfVersion(engine, 8);
    Scan scan = currSnapshot.getScanBuilder(engine).build();
    Row scanState = scan.getScanState(engine);
    List<ScanFile> scanFiles = new ArrayList<>();

    for (String r : v11) {
      ScanFile scanFile = new ScanFile(RowSerDe.serializeRowToJson(scanState), r);
      scanFiles.add(scanFile);
    }
    System.out.println(v10.get(0));
    System.out.println(v11.get(0));
    System.out.println(scanFiles.size());

    CloseableIterator<ColumnarBatch> fileIter =
        ((TableImpl) table)
            .getChanges(engine, 9, 10, Set.of(DeltaLogActionUtils.DeltaAction.ADD));
    List<String> files = new ArrayList<>();
    while (fileIter.hasNext()) {
      try (CloseableIterator<Row> scanFileRows = fileIter.next().getRows()) {
        while (scanFileRows.hasNext()) {
          files.add(RowSerDe.serializeRowToJson(scanFileRows.next()));
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    System.out.println(files.get(0));
    System.out.println(files.size());
  }

  private static List<ScanFile> getScanFiles(Table table, Engine engine, long version) {
    List<String> versionFiles = getScannedFiles(table, engine, version);
    List<String> prevVersionFiles =
        version == 0 ? new ArrayList<>() : getScannedFiles(table, engine, version - 1);
    versionFiles.removeAll(prevVersionFiles);

    Snapshot currSnapshot = table.getSnapshotAsOfVersion(engine, version);
    Scan scan = currSnapshot.getScanBuilder(engine).build();
    Row scanState = scan.getScanState(engine);
    List<ScanFile> scanFiles = new ArrayList<>();

    for (String r : versionFiles) {
      ScanFile scanFile = new ScanFile(RowSerDe.serializeRowToJson(scanState), r);
      scanFiles.add(scanFile);
    }
    return scanFiles;
  }

  private static List<String> getScannedFiles(Table table, Engine engine, long version) {
    Snapshot currSnapshot = table.getSnapshotAsOfVersion(engine, version);
    Scan scan = currSnapshot.getScanBuilder(engine).build();
    Row scanState = scan.getScanState(engine);
    CloseableIterator<FilteredColumnarBatch> fileIter = scan.getScanFiles(engine);

    List<String> files = new ArrayList<>();
    while (fileIter.hasNext()) {
      try (CloseableIterator<Row> scanFileRows = fileIter.next().getRows()) {
        while (scanFileRows.hasNext()) {
          files.add(RowSerDe.serializeRowToJson(scanFileRows.next()));
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return files;
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
        .map(
            r -> {
              try {
                return objectMapper.readValue(r, TableFileState.class);
              } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
              }
            })
        .collect(Collectors.toList());
  }

  public List<TableFileState> getFilesForCurrentPartition(Integer workerId) {
    List<TableFileState> stateMap = getCurrentStateMap();
    List<TableFileState> partitionMap = new ArrayList<>();

    int numWorkers = this.numWorkers.get();
    for (int i = 0; i < stateMap.size(); i++) {
      if (stateMap.get(i).getState() == TableFileState.State.PENDING && i % numWorkers == workerId)
        partitionMap.add(stateMap.get(i));
    }
    return partitionMap;
  }

  public void updateWorkers(Integer numWorkers) {
    this.numWorkers.set(numWorkers);
  }

  @SneakyThrows
  private List<String> initStateMap(List<ScanFile> files) {
    List<String> map = new ArrayList<>();
    for (int i = 0; i < files.size(); i++) {
      map.add(
          objectMapper.writeValueAsString(
              TableFileState.builder()
                  .fileId(i)
                  .scanFile(files.get(i))
                  .state(TableFileState.State.PENDING)
                  .build()));
    }
    return map;
  }

  public Completable rxUpdateFileState(Integer fileId, TableFileState.State state) {
    long start = System.currentTimeMillis();
    return Completable.create(
        emitter -> {
          AtomicInteger trying = new AtomicInteger(0);
          vertx.setPeriodic(
              2_000,
              timerId -> {
                if (trying.compareAndSet(0, 1)) {
                  if (updateFileState(fileId, state)) {
                    vertx.cancelTimer(timerId);
                    emitter.onComplete();
                  } else if (System.currentTimeMillis() - start >= 60_000) {
                    vertx.cancelTimer(timerId);
                    emitter.onError(new RuntimeException("timed out waiting to update file state"));
                  } else {
                    trying.set(0);
                  }
                }
              });
        });
  }

  public boolean updateFileState(Integer fileId, TableFileState.State state) {
    Stat stat = new Stat();
    ZNRecord record =
        manager
            .getHelixPropertyStore()
            .get(
                String.format("/METADATA_ROOT_PATH/%s/STATE_MAP", tableName),
                stat,
                AccessOption.PERSISTENT);
    List<TableFileState> stateMap =
        record.getListField("current-state").stream()
            .map(
                r -> {
                  try {
                    return objectMapper.readValue(r, TableFileState.class);
                  } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toList());
    stateMap.get(fileId).setState(state);
    List<String> updated =
        stateMap.stream()
            .map(
                r -> {
                  try {
                    return objectMapper.writeValueAsString(r);
                  } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toList());
    record.setListField("current-state", updated);
    return manager
        .getHelixPropertyStore()
        .set(
            String.format("/METADATA_ROOT_PATH/%s/STATE_MAP", tableName),
            record,
            stat.getVersion(),
            AccessOption.PERSISTENT);
  }

  private boolean isCurrentVersionDone(List<TableFileState> fileStates) {
    for (TableFileState fileState : fileStates) {
      if (!fileState.getState().equals(TableFileState.State.COMPLETED)) return false;
    }
    return true;
  }

  @SneakyThrows
  public Flowable<Row> rxProcessBatch(String state, String file) {
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

    return Flowable.<FilteredColumnarBatch>generate(
            emitter -> {
              CloseableIterator<FilteredColumnarBatch> dataIter =
                  Scan.transformPhysicalData(engine, scanState, scanFile, physicalDataIter);
              while (dataIter.hasNext()) emitter.onNext(dataIter.next());
              emitter.onComplete();
            })
        .flatMap(
            batch ->
                Flowable.generate(
                    emitter -> {
                      CloseableIterator<Row> rows = batch.getRows();
                      while (rows.hasNext()) {
                        emitter.onNext(rows.next());
                      }
                      emitter.onComplete();
                    }));
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
                new FileWriter(String.format("target/output-%s.txt", UUID.randomUUID())))) {
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
