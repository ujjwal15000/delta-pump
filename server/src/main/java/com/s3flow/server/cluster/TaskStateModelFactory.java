package com.s3flow.server.cluster;

import lombok.extern.slf4j.Slf4j;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.Message;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.task.Task;

@Slf4j
public class TaskStateModelFactory extends StateModelFactory<StateModel> {
  private final String workerId;
  private final TaskFactory taskFactory;

  public TaskStateModelFactory(String workerId, TaskFactory taskFactory) {
    this.workerId = workerId;
    this.taskFactory = taskFactory;
  }

  @Override
  public TaskStateModel createNewStateModel(String resource, String partition) {
    return new TaskStateModel(workerId, partition, taskFactory);
  }

  public static class TaskStateModel extends StateModel {
    private final String workerId;
    private final String partition;
    private final TaskFactory taskFactory;

    public TaskStateModel(String workerId, String partition, TaskFactory taskFactory) {
      this.workerId = workerId;
      this.partition = partition;
      this.taskFactory = taskFactory;
    }

    // Transition from INIT to RUNNING
    public void onBecomeRunningFromInit(Message message, NotificationContext context) throws Exception {
      log.info("{}: Transitioning from INIT to RUNNING for partition {}", workerId, partition);
      executeTask(message, context);
    }

    // Transition from RUNNING to COMPLETED
    public void onBecomeCompletedFromRunning(Message message, NotificationContext context) {
      log.info("{}: Transitioning from RUNNING to COMPLETED for partition {}", workerId, partition);
      // Clean up or mark task as completed
    }

    // Transition from RUNNING to STOPPED
    public void onBecomeStoppedFromRunning(Message message, NotificationContext context) {
      log.warn("{}: Transitioning from RUNNING to STOPPED for partition {}", workerId, partition);
      // Handle graceful stop of the task
    }

    // Transition from RUNNING to TIMED_OUT
    public void onBecomeTimedOutFromRunning(Message message, NotificationContext context) {
      log.error("{}: Transitioning from RUNNING to TIMED_OUT for partition {}", workerId, partition);
      // Handle task timeout logic
    }

    // Transition from RUNNING to TASK_ERROR
    public void onBecomeTaskErrorFromRunning(Message message, NotificationContext context) {
      log.error("{}: Transitioning from RUNNING to TASK_ERROR for partition {}", workerId, partition);
      // Handle error state
    }

    // Transition from INIT to DROPPED
    public void onBecomeDroppedFromInit(Message message, NotificationContext context) {
      log.info("{}: Transitioning from INIT to DROPPED for partition {}", workerId, partition);
      // Handle dropping of the task
    }

    // Transition from any state to RESET
    @Override
    public void reset() {
      log.warn("{}: Reset invoked for partition {}", workerId, partition);
      // Reset logic for the partition
    }

    private void executeTask(Message message, NotificationContext context) throws Exception {
      HelixManager manager = context.getManager();
      ConfigAccessor clusterConfig = manager.getConfigAccessor();
      HelixConfigScope clusterScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER)
              .forCluster(manager.getClusterName())
              .build();

      String resourceName = message.getResourceName();
      Task task = taskFactory.createTask(resourceName, manager);

      log.info("{}: Starting task for partition {}", workerId, partition);
      try {
        task.run();
        log.info("{}: Task for partition {} completed successfully", workerId, partition);
      } catch (Exception e) {
        log.error("{}: Task execution failed for partition {}", workerId, partition, e);
        throw e;
      }
    }
  }
}
