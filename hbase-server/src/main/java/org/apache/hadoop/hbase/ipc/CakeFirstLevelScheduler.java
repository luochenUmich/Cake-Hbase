package org.apache.hadoop.hbase.ipc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.DaemonThreadFactory;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.util.StringUtils;

import com.google.protobuf.Message;

@InterfaceAudience.LimitedPrivate({ HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX })
@InterfaceStability.Evolving
public class CakeFirstLevelScheduler extends RpcScheduler {

  private static final Log LOG = LogFactory.getLog(CakeFirstLevelScheduler.class);

  private final AtomicInteger failedHandlerCount = new AtomicInteger(0);

  private final Configuration conf;
  private final Abortable abortable;

  private final int LOW_PRIORITY_INDEX = 0;
  private final int HIGH_PRIORITY_INDEX = 1;
  private final int numQueues = 2;

  List<BlockingQueue<Runnable>> callQueues = new ArrayList<>();
  List<ThreadPoolExecutor> executors = new ArrayList<>();
  List<Integer> numHandlers = new ArrayList<>();

  private int numAllHandlers;

  public CakeFirstLevelScheduler(Configuration conf, Abortable abortable) {
    this.conf = conf;
    this.abortable = abortable;
  }

  @Override
  public void init(Context context) {
    numAllHandlers = CakeConstants.MAX_HBASE_HANDLERS;
    setHighPriorityClientShare(CakeConstants.HIGH_PRIORITY_INITIAL_SHARE);
    for (int i = 0; i < numQueues; ++i) {
      callQueues.add(new LinkedBlockingQueue<Runnable>());
    }
  }

  @Override
  public void start() {
    for (int i = 0; i < numQueues; ++i) {
      this.executors
          .add(new ThreadPoolExecutor(numHandlers.get(i), numHandlers.get(i), 60, TimeUnit.SECONDS,
              callQueues.get(i), new DaemonThreadFactory("CakeHighPriorityRpcScheduler.handler"),
              new ThreadPoolExecutor.CallerRunsPolicy()));
    }
  }

  @Override
  public void stop() {
    for (ThreadPoolExecutor executor : executors) {
      executor.shutdown();
    }
  }

  @Override
  public void dispatch(final CallRunner task) throws IOException, InterruptedException {
    Message request = task.getCall().param;
    int index = 0;
    if (request instanceof GetRequest) {
      index = HIGH_PRIORITY_INDEX;
    } else if (request instanceof ScanRequest) {
      index = LOW_PRIORITY_INDEX;
    }
    LOG.debug("Start executing task on Cake executor");
    executors.get(index).submit(new Runnable() {
      @Override
      public void run() {
        double handlerFailureThreshhold = conf == null ? 1.0
            : conf.getDouble(HConstants.REGION_SERVER_HANDLER_ABORT_ON_ERROR_PERCENT,
              HConstants.DEFAULT_REGION_SERVER_HANDLER_ABORT_ON_ERROR_PERCENT);
        task.setStatus(RpcServer.getStatus());
        try {
          task.run();
        } catch (Throwable e) {
          if (e instanceof Error) {
            int failedCount = failedHandlerCount.incrementAndGet();
            if (handlerFailureThreshhold >= 0
                && failedCount > CakeConstants.MAX_HBASE_HANDLERS * handlerFailureThreshhold) {
              String message =
                  "Number of failed Cake Rpchandler exceeded threshhold " + handlerFailureThreshhold
                      + "  with failed reason: " + StringUtils.stringifyException(e);
              if (abortable != null) {
                abortable.abort(message, e);
              } else {
                LOG.error("Received " + StringUtils.stringifyException(e)
                    + " but not aborting due to abortable being null");
                throw e;
              }
            } else {
              LOG.warn(
                "Cake Rpc handler threads encountered errors " + StringUtils.stringifyException(e));
            }
          } else {
            LOG.warn("Cake Rpc handler threads encountered exceptions "
                + StringUtils.stringifyException(e));
          }
        }
      }
    });
  }

  @Override
  public int getGeneralQueueLength() {
    return callQueues.get(LOW_PRIORITY_INDEX).size() + callQueues.get(HIGH_PRIORITY_INDEX).size();
  }

  @Override
  public int getPriorityQueueLength() {
    return 0;
  }

  @Override
  public int getReplicationQueueLength() {
    return 0;
  }

  @Override
  public int getActiveRpcHandlerCount() {
    return executors.get(LOW_PRIORITY_INDEX).getActiveCount()
        + executors.get(HIGH_PRIORITY_INDEX).getActiveCount();
  }

  void setHighPriorityClientShare(double highPriorityClientShare) {
    numHandlers.set(HIGH_PRIORITY_INDEX, (int) (numAllHandlers * highPriorityClientShare));
    numHandlers.set(LOW_PRIORITY_INDEX, numAllHandlers - numHandlers.get(HIGH_PRIORITY_INDEX));
    for (int i = 0; i < executors.size(); ++i) {
      executors.get(i).setCorePoolSize(numHandlers.get(i));
      executors.get(i).setMaximumPoolSize(numHandlers.get(i));
    }
  }
}
