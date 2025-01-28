
package com.s3flow.server.cluster;

import lombok.extern.slf4j.Slf4j;
import org.apache.helix.HelixManager;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskResult;

@Slf4j
public class TaskFactory {
  public Task createTask(String id, HelixManager helixManager){
    return new Task() {
      @Override
      public TaskResult run() {
        log.info("running task for id {}", id);
        return new TaskResult(TaskResult.Status.COMPLETED, "done");
      }

      @Override
      public void cancel() {
        log.info("cancelling task for id {}", id);
      }
    };
  }
}