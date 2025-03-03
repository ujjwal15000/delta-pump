package com.deltapump.server.deltareader;

import com.deltapump.server.model.TableFileState;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.*;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.internal.TableImpl;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.FileStatus;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
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

import java.io.IOException;
import java.net.URI;
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

  private static List<ScanFile> getScanFiles(Table table, Engine engine, long version) {
    Snapshot currSnapshot = table.getSnapshotAsOfVersion(engine, version);
    Scan scan = currSnapshot.getScanBuilder(engine).build();
    Row scanState = scan.getScanState(engine);
    List<ScanFile> scanFiles = new ArrayList<>();

    CloseableIterator<ColumnarBatch> fileIter =
        ((TableImpl) table)
            .getChanges(engine, version, version, Set.of(DeltaLogActionUtils.DeltaAction.ADD));

    while (fileIter.hasNext()) {
      try (CloseableIterator<Row> scanFileRows = fileIter.next().getRows()) {
        while (scanFileRows.hasNext()) {
          Row scanFileRow = scanFileRows.next();
          if (!scanFileRow.isNullAt(
              scanFileRow.getSchema().indexOf(DeltaLogActionUtils.DeltaAction.ADD.colName)))
            scanFiles.add(
                new ScanFile(
                    RowSerDe.serializeRowToJson(scanState),
                    RowSerDe.serializeRowToJson(scanFileRow)));
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return scanFiles;
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

    Row addFile =
        scanFile.getStruct(
            scanFile.getSchema().indexOf(DeltaLogActionUtils.DeltaAction.ADD.colName));
    String path = addFile.getString(addFile.getSchema().indexOf("path"));
    long size = addFile.getLong(addFile.getSchema().indexOf("size"));
    long modificationTime = addFile.getLong(addFile.getSchema().indexOf("modificationTime"));
    String absolutePath =
        new Path(new Path(URI.create(table.getPath(engine))), new Path(URI.create(path)))
            .toString();

    FileStatus fileStatus = FileStatus.of(absolutePath, size, modificationTime);
    StructType physicalReadSchema = ScanStateRow.getPhysicalDataReadSchema(engine, scanState);

    CloseableIterator<ColumnarBatch> physicalDataIter =
        engine
            .getParquetHandler()
            .readParquetFiles(
                singletonCloseableIterator(fileStatus), physicalReadSchema, Optional.empty());

    return getFlowableFromClosableIterator(physicalDataIter)
        .map(ColumnarBatch::getRows)
        .flatMap(TableReader::getFlowableFromClosableIterator);
  }

  public static <T> Flowable<T> getFlowableFromClosableIterator(CloseableIterator<T> iterator) {
    return Flowable.generate(
        () -> iterator,
        (iter, emitter) -> {
          if (iter.hasNext()) {
            emitter.onNext(iter.next());
          } else {
            emitter.onComplete();
          }
          return iter;
        },
        CloseableIterator::close);
  }
}
