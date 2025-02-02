
package com.deltapump.server.cluster;

import com.deltapump.server.deltareader.TableReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskResult;

@Slf4j
public class TaskFactory {
  public Task createTask(String id, String state, String file, TableReader tableReader){
    return new Task() {
      @Override
      public TaskResult run() {
        log.info("running task for id {}", id);
        tableReader.processBatch(state, file);
        return new TaskResult(TaskResult.Status.COMPLETED, "done");
      }

      @Override
      public void cancel() {
        log.info("cancelling task for id {}", id);
      }
    };
  }
}