package com.deltapump.server.cluster;

import com.deltapump.server.FlowServer;
import com.deltapump.server.deltareader.TableReader;
import io.vertx.rxjava3.core.Vertx;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.helix.*;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.task.*;

import java.util.Map;

import static org.apache.helix.task.TaskResult.Status.COMPLETED;

@Slf4j
public class TaskStateModelFactory extends StateModelFactory<StateModel> {
  private final Vertx vertx;
  private final String instanceId;
  private final TaskFactory taskFactory;

  public TaskStateModelFactory(Vertx vertx, String instanceId, TaskFactory taskFactory) {
    this.vertx = vertx;
    this.instanceId = instanceId;
    this.taskFactory = taskFactory;
  }

  @Override
  public TaskStateModel createNewStateModel(String resource, String partition) {
    return new TaskStateModel(vertx, instanceId, partition, taskFactory);
  }

  public static class TaskStateModel extends org.apache.helix.task.TaskStateModel {
    private final String instanceId;
    private final String partition;
    private final TaskFactory taskFactory;
    private final Vertx vertx;
    private final TableReader tableReader;

    public TaskStateModel(Vertx vertx, String workerId, String partition, TaskFactory taskFactory) {
        super(null, null, null, null);
        this.vertx = vertx;
      this.tableReader =
          (TableReader) vertx.sharedData().getLocalMap(FlowServer.SHARED_MAP).get(TableReader.class.getName());
      this.instanceId = workerId;
      this.partition = partition;
      this.taskFactory = taskFactory;
    }

    @Transition(to = "RUNNING", from = "INIT")
    public void onBecomeRunningFromInit(Message message, NotificationContext context) {
      log.info("{}: Transitioning from INIT to RUNNING for partition {}", instanceId, partition);
      executeTask(message, context);
    }

    @Transition(to = "STOPPED", from = "RUNNING")
    public String onBecomeStoppedFromRunning(Message msg, NotificationContext context) {return "";}

    @Transition(to = "COMPLETED", from = "RUNNING")
    public String onBecomeCompletedFromRunning(Message msg, NotificationContext context) {return "";}

    @Transition(to = "TIMED_OUT", from = "RUNNING")
    public String onBecomeTimedOutFromRunning(Message msg, NotificationContext context) {return "";}

    @Transition(to = "TASK_ERROR", from = "RUNNING")
    public String onBecomeTaskErrorFromRunning(Message msg, NotificationContext context) {return "";}

    @Transition(to = "TASK_ABORTED", from = "RUNNING")
    public String onBecomeTaskAbortedFromRunning(Message msg, NotificationContext context) {return "";}

    @Transition(to = "RUNNING", from = "STOPPED")
    public void onBecomeRunningFromStopped(Message msg, NotificationContext context) {}

    @Transition(to = "DROPPED", from = "INIT")
    public void onBecomeDroppedFromInit(Message msg, NotificationContext context) {
      reset();
    }

    @Transition(to = "DROPPED", from = "RUNNING")
    public void onBecomeDroppedFromRunning(Message msg, NotificationContext context) {}

    @Transition(to = "DROPPED", from = "COMPLETED")
    public void onBecomeDroppedFromCompleted(Message msg, NotificationContext context) {
      reset();
    }

    @Transition(to = "DROPPED", from = "STOPPED")
    public void onBecomeDroppedFromStopped(Message msg, NotificationContext context) {
      reset();
    }

    @Transition(to = "DROPPED", from = "TIMED_OUT")
    public void onBecomeDroppedFromTimedOut(Message msg, NotificationContext context) {
      reset();
    }

    @Transition(to = "DROPPED", from = "TASK_ERROR")
    public void onBecomeDroppedFromTaskError(Message msg, NotificationContext context) {
      reset();
    }

    @Transition(to = "DROPPED", from = "TASK_ABORTED")
    public void onBecomeDroppedFromTaskAborted(Message msg, NotificationContext context) {
      reset();
    }

    @Transition(to = "INIT", from = "RUNNING")
    public void onBecomeInitFromRunning(Message msg, NotificationContext context) {reset();}

    @Transition(to = "INIT", from = "COMPLETED")
    public void onBecomeInitFromCompleted(Message msg, NotificationContext context) {
      reset();
    }

    @Transition(to = "INIT", from = "STOPPED")
    public void onBecomeInitFromStopped(Message msg, NotificationContext context) {
      reset();
    }

    @Transition(to = "INIT", from = "TIMED_OUT")
    public void onBecomeInitFromTimedOut(Message msg, NotificationContext context) {
      reset();
    }

    @Transition(to = "INIT", from = "TASK_ERROR")
    public void onBecomeInitFromTaskError(Message msg, NotificationContext context) {
      reset();
    }

    @Transition(to = "INIT", from = "TASK_ABORTED")
    public void onBecomeInitFromTaskAborted(Message msg, NotificationContext context) {
      reset();
    }

    @Override
    public void reset() {
      log.warn("{}: Reset invoked for partition {}", instanceId, partition);
    }

    @SneakyThrows
    private void executeTask(Message message, NotificationContext context) {
      HelixManager manager = context.getManager();

      String resource = message.getResourceName();
      PropertyKey.Builder keyBuilder = manager.getHelixDataAccessor().keyBuilder();
      JobConfig config =
          new JobConfig(
              manager.getHelixDataAccessor().getProperty(keyBuilder.resourceConfig(resource)));

      String partitionId = partition.split("_")[partition.split("_").length - 1];
      Map<String, String> configMap = config.getTaskConfigMap().get(partitionId).getConfigMap();
      Task task =
          taskFactory.createTask(
              resource, configMap.get("state"), configMap.get("file"), tableReader);
      log.info("{}: Starting task for partition {}", instanceId, partition);
      try {
        TaskResult result = task.run();
        if (result.getStatus() != COMPLETED) {
          updateTaskState(
              manager, message.getTgtSessionId(), resource, TaskPartitionState.ERROR, result);
          throw new RuntimeException("task failed");
        }
        updateTaskState(
            manager, message.getTgtSessionId(), resource, TaskPartitionState.COMPLETED, result);
        log.info("{}: Task for partition {} completed successfully", instanceId, partition);
      } catch (Exception e) {
        log.error("{}: Task execution failed for partition {}", instanceId, partition, e);
      }
    }

    private boolean updateTaskState(
        HelixManager manager,
        String sessionId,
        String resource,
        TaskPartitionState state,
        TaskResult result) {
//      return true;
      PropertyKey.Builder keyBuilder = manager.getHelixDataAccessor().keyBuilder();
      PropertyKey key =
          Boolean.getBoolean(SystemPropertyKeys.TASK_CURRENT_STATE_PATH_DISABLED)
              ? keyBuilder.currentState(instanceId, sessionId, resource)
              : keyBuilder.taskCurrentState(instanceId, sessionId, resource);

      String prevState = this.getCurrentState();
      CurrentState currentStateDelta = new CurrentState(resource);
      currentStateDelta.setSessionId(sessionId);
      currentStateDelta.setStateModelDefRef(TaskConstants.STATE_MODEL_NAME);
      currentStateDelta.setState(partition, state.name());
      currentStateDelta.setInfo(partition, result.getInfo());
      currentStateDelta.setPreviousState(partition, prevState);
      boolean success = manager.getHelixDataAccessor().updateProperty(key, currentStateDelta);
      if(success)
        this.updateState(state.name());
      return success;
    }
  }
}
