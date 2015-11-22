package org.apache.hadoop.hbase.ipc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.util.StringUtils;

import com.google.protobuf.Message;

@InterfaceAudience.LimitedPrivate({ HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX })
@InterfaceStability.Evolving
public class CakeFirstLevelScheduler extends RpcScheduler {

  private static final Log LOG = LogFactory.getLog(CakeFirstLevelScheduler.class);

  private final AtomicInteger activeHandlerCount = new AtomicInteger(0);

  BlockingQueue<CallRunner> highPriorityQueue;
  BlockingQueue<CallRunner> lowPriorityQueue;

  private AtomicInteger numHandlers;
  private boolean running;

  private AtomicInteger totalHighPriorityHandlers;
  private AtomicInteger totalLowPriorityHandlers;

  private AtomicInteger currHighPriorityHandlers = new AtomicInteger(0);
  private AtomicInteger currLowPriorityHandlers = new AtomicInteger(0);

  private List<Thread> handlers;

  @Override
  public void init(Context context) {
    numHandlers = new AtomicInteger(CakeConstants.MAX_HBASE_HANDLERS);
    handlers = new ArrayList<Thread>(numHandlers.get());
    setHighPriorityClientShare(CakeConstants.HIGH_PRIORITY_INITIAL_SHARE);
  }

  @Override
  public void start() {
    running = true;
    startHandlers();
  }

  private void startHandlers() {
    for (int i = 0; i < numHandlers.get(); i++) {
      final BlockingQueue<Thread> queue = null
      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
          if (i % 2 == 0 && !currHighPriorityHandlers.equals(totalHighPriorityHandlers)) {
            consumerLoop(hight);
          } 
        }
      });
      t.setDaemon(true);
      t.start();
      LOG.debug("Cake RPC handlers starts");
      handlers.add(t);
    }
  }

  private void consumerLoop(final BlockingQueue<CallRunner> myQueue) {
    boolean interrupted = false;
    double handlerFailureThreshhold = conf == null ? 1.0
        : conf.getDouble(HConstants.REGION_SERVER_HANDLER_ABORT_ON_ERROR_PERCENT,
          HConstants.DEFAULT_REGION_SERVER_HANDLER_ABORT_ON_ERROR_PERCENT);
    try {
      while (running) {
        try {
          MonitoredRPCHandler status = RpcServer.getStatus();
          CallRunner task = myQueue.take();
          task.setStatus(status);
          try {
            activeHandlerCount.incrementAndGet();
            task.run();
          } catch (Throwable e) {
            if (e instanceof Error) {
              int failedCount = failedHandlerCount.incrementAndGet();
              if (handlerFailureThreshhold >= 0
                  && failedCount > handlerCount * handlerFailureThreshhold) {
                String message = "Number of failed RpcServer handler exceeded threshhold "
                    + handlerFailureThreshhold + "  with failed reason: "
                    + StringUtils.stringifyException(e);
                if (abortable != null) {
                  abortable.abort(message, e);
                } else {
                  LOG.error("Received " + StringUtils.stringifyException(e)
                      + " but not aborting due to abortable being null");
                  throw e;
                }
              } else {
                LOG.warn("RpcServer handler threads encountered errors "
                    + StringUtils.stringifyException(e));
              }
            } else {
              LOG.warn("RpcServer handler threads encountered exceptions "
                  + StringUtils.stringifyException(e));
            }
          } finally {
            activeHandlerCount.decrementAndGet();
          }
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public void stop() {
    running = false;
    for (Thread handler : handlers) {
      handler.interrupt();
    }
  }

  @Override
  public void dispatch(CallRunner task) throws IOException, InterruptedException {
    Message request = task.getCall().param;
    if (request instanceof GetRequest) {
      highPriorityQueue.put(task);
    } else if (request instanceof ScanRequest) {
      lowPriorityQueue.put(task);
    }
  }

  @Override
  public int getGeneralQueueLength() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getPriorityQueueLength() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getReplicationQueueLength() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getActiveRpcHandlerCount() {
    // TODO Auto-generated method stub
    return 0;
  }

  void setHighPriorityClientShare(double highPriorityClientShare) {
    totalHighPriorityHandlers.set((int) (numHandlers.get() * highPriorityClientShare));
    totalLowPriorityHandlers.set(numHandlers.get() - totalHighPriorityHandlers.get());
  }
}
