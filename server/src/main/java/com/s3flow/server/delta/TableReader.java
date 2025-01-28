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
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import io.delta.kernel.utils.CloseableIterator;

import java.io.IOException;
import java.util.Optional;

import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;

public class TableReader {
  Engine myEngine;

  public static void main(String[] args) {
    TableReader reader = new TableReader();
  }

  public TableReader() {
    String myTablePath = "/Users/ujjwalbagrania/Desktop/notebooks/delta";
    Configuration hadoopConf = new Configuration();
    myEngine = DefaultEngine.create(hadoopConf);
    Table myTable = Table.forPath(myEngine, myTablePath);

    Snapshot snapshot = myTable.getLatestSnapshot(myEngine);

    long version = snapshot.getVersion(myEngine);
    System.out.println("version: " + version);

    StructType tableSchema = snapshot.getSchema(myEngine);
    System.out.println("schema: " + tableSchema);

    Scan scan = snapshot.getScanBuilder(myEngine).build();

    Row scanState = scan.getScanState(myEngine);
    CloseableIterator<FilteredColumnarBatch> fileIter = scan.getScanFiles(myEngine);
    while (fileIter.hasNext()) {
      try (CloseableIterator<Row> scanFileRows = fileIter.next().getRows()) {
        while (scanFileRows.hasNext()) {
          processBatch(new ScanFile(scanState, scanFileRows.next()));
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @SneakyThrows
  private void processBatch(ScanFile work) {
    Row scanState = work.getScanRow(myEngine);
    Row scanFile = work.getScanFileRow(myEngine);
    FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFile);
    StructType physicalReadSchema = ScanStateRow.getPhysicalDataReadSchema(myEngine, scanState);

    CloseableIterator<ColumnarBatch> physicalDataIter =
        myEngine
            .getParquetHandler()
            .readParquetFiles(
                singletonCloseableIterator(fileStatus), physicalReadSchema, Optional.empty());

    try (CloseableIterator<FilteredColumnarBatch> dataIter =
        Scan.transformPhysicalData(myEngine, scanState, scanFile, physicalDataIter)) {
      while (dataIter.hasNext()) {
        FilteredColumnarBatch batch = dataIter.next();
        CloseableIterator<Row> rows = batch.getRows();
        while(rows.hasNext()){
          Row row = rows.next();
          System.out.println(row.getString(0) + " : " + row.getString(1));
        }
      }
    } catch (Exception e) {
    }
  }

  private static class ScanFile {
    private static final ScanFile POISON_PILL = new ScanFile("", "");

    final String stateJson;
    final String fileJson;

    ScanFile(Row scanStateRow, Row scanFileRow) {
      this.stateJson = RowSerDe.serializeRowToJson(scanStateRow);
      this.fileJson = RowSerDe.serializeRowToJson(scanFileRow);
    }

    ScanFile(String stateJson, String fileJson) {
      this.stateJson = stateJson;
      this.fileJson = fileJson;
    }

    Row getScanRow(Engine engine) {
      return RowSerDe.deserializeRowFromJson(stateJson);
    }

    Row getScanFileRow(Engine engine) {
      return RowSerDe.deserializeRowFromJson(fileJson);
    }
  }
}
