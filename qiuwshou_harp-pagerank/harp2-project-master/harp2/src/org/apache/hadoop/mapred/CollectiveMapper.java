/*
 * Copyright 2014 Indiana University
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import it.unimi.dsi.fastutil.ints.Int2IntMap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;

import edu.iu.harp.client.fork.EventFuture;
import edu.iu.harp.client.fork.SyncClient;
import edu.iu.harp.collective.AllgatherCollective;
import edu.iu.harp.collective.AllreduceCollective;
import edu.iu.harp.collective.BcastCollective;
import edu.iu.harp.collective.GraphCollective;
import edu.iu.harp.collective.LocalGlobalSyncCollective;
import edu.iu.harp.collective.ReduceCollective;
import edu.iu.harp.collective.RegroupCollective;
import edu.iu.harp.comm.Communication;
import edu.iu.harp.event.Event;
import edu.iu.harp.event.EventType;
import edu.iu.harp.io.ConnectionPool;
import edu.iu.harp.io.Constants;
import edu.iu.harp.io.DataMap;
import edu.iu.harp.io.EventQueue;
import edu.iu.harp.partition.Partition;
import edu.iu.harp.partition.PartitionFunction;
import edu.iu.harp.partition.PartitionUtils;
import edu.iu.harp.partition.Table;
import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.server.reuse.Server;
import edu.iu.harp.worker.Workers;

/**
 * CollectiveMapper is extended from original
 * mapper in Hadoop. It includes new APIs for
 * in-memory collective communication.
 * 
 * @author zhangbj
 * 
 * @param <KEYIN>
 *          Input key
 * @param <VALUEIN>
 *          Input value
 * @param <KEYOUT>
 *          Output key
 * @param <VALUEOUT>
 *          Output value
 */
public class CollectiveMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
  extends
  Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  protected static final Log LOG = LogFactory
    .getLog(CollectiveMapper.class);

  private int workerID;
  private Workers workers;
  private ResourcePool resourcePool;
  private EventQueue eventQueue;
  private DataMap dataMap;
  private Server server;
  private SyncClient client;

  /**
   * A Key-Value reader to read key-value inputs
   * for this worker.
   *
   * @author zhangbj
   */
  protected class KeyValReader {
    private Context context;

    protected KeyValReader(Context context) {
      this.context = context;
    }

    public boolean nextKeyValue()
      throws IOException, InterruptedException {
      return this.context.nextKeyValue();
    }

    public KEYIN getCurrentKey()
      throws IOException, InterruptedException {
      return this.context.getCurrentKey();
    }

    public VALUEIN getCurrentValue()
      throws IOException, InterruptedException {
      return this.context.getCurrentValue();
    }
  }

  /**
   * If lock file couldn't be gotten, quit.
   * 
   * @param lockFile
   * @param fs
   * @return
   */
  private boolean tryLockFile(String lockFile,
    FileSystem fs) {
    LOG.info("TRY LOCK FILE " + lockFile + " "
      + fs.getHomeDirectory());
    Path path =
      new Path(fs.getHomeDirectory(), lockFile);
    boolean retry = false;
    int retryCount = 0;
    do {
      try {
        retry = !fs.exists(path);
      } catch (Exception e) {
        LOG.error("Read lock file exception.", e);
        retry = true;
      }
      if (retry) {
        try {
          Thread.sleep(Constants.SHORT_SLEEP);
        } catch (InterruptedException e) {
        }
        retryCount++;
        LOG.info("Fail to read nodes lock file "
          + path.toString()
          + ", retry... Retry count: "
          + retryCount);
        if (retryCount == Constants.LARGE_RETRY_COUNT) {
          return false;
        }
      }
    } while (retry);
    return true;
  }

  private Map<Integer, Integer> getTaskWorkerMap(
    String tasksFile, FileSystem fs) {
    LOG.info("Get task file " + tasksFile);
    Map<Integer, Integer> taskWorkerMap = null;
    Path path =
      new Path(fs.getHomeDirectory(), tasksFile);
    try {
      taskWorkerMap =
        new TreeMap<Integer, Integer>();
      FSDataInputStream in = fs.open(path);
      BufferedReader br =
        new BufferedReader(new InputStreamReader(
          in));
      String line = null;
      String[] tokens = null;
      while ((line = br.readLine()) != null) {
        tokens = line.split("\t");
        taskWorkerMap.put(
          Integer.parseInt(tokens[0]),
          Integer.parseInt(tokens[1]));
      }
      br.close();
    } catch (IOException e) {
      LOG.error("No TASK FILE FOUND");
      taskWorkerMap = null;
    }
    return taskWorkerMap;
  }

  private BufferedReader getNodesReader(
    String nodesFile, FileSystem fs)
    throws IOException {
    LOG.info("Get nodes file " + nodesFile);
    Path path =
      new Path(fs.getHomeDirectory(), nodesFile);
    FSDataInputStream in = fs.open(path);
    BufferedReader br =
      new BufferedReader(
        new InputStreamReader(in));
    return br;
  }

  private boolean initCollCommComponents(
    Context context) throws IOException {
    // Get file names
    String jobDir = context.getJobID().toString();
    String nodesFile = jobDir + "/nodes";
    String tasksFile = jobDir + "/tasks";
    String lockFile = jobDir + "/lock";
    FileSystem fs =
      FileSystem.get(context.getConfiguration());
    // Try lock
    boolean isSuccess = tryLockFile(lockFile, fs);
    if (!isSuccess) {
      return false;
    }
    Map<Integer, Integer> taskWorkerMap =
      getTaskWorkerMap(tasksFile, fs);
    // Get worker ID
    int taskID =
      context.getTaskAttemptID().getTaskID()
        .getId();
    LOG.info("Task ID " + taskID);
    if (taskWorkerMap == null) {
      workerID = taskID;
    } else {
      workerID = taskWorkerMap.get(taskID);
    }
    LOG.info("WORKER ID: " + workerID);
    // Get nodes file and initialize workers
    BufferedReader br =
      getNodesReader(nodesFile, fs);
    try {
      workers = new Workers(br, workerID);
      br.close();
    } catch (Exception e) {
      LOG.error("Cannot initialize workers.", e);
      throw new IOException(e);
    }
    eventQueue = new EventQueue();
    dataMap = new DataMap();
    resourcePool = new ResourcePool();
    client =
      new SyncClient(workers, resourcePool,
        Constants.NUM_SEND_THREADS);
    // Initialize receiver
    String host = workers.getSelfInfo().getNode();
    int port = workers.getSelfInfo().getPort();
    try {
      server =
        new Server(host, port,
          Constants.NUM_RECV_THREADS, eventQueue,
          dataMap, workers, resourcePool);
    } catch (Exception e) {
      LOG
        .error("Cannot initialize receivers.", e);
      throw new IOException(e);
    }
    client.start();
    server.start();
    context.getProgress();
    isSuccess =
      barrier("start-worker", "handshake");
    LOG.info("Barrier: " + isSuccess);
    return isSuccess;
  }

  /**
   * Get the ID of this worker.
   * 
   * @return Worker ID
   */
  public int getSelfID() {
    return this.workerID;
  }

  public int getMasterID() {
    return this.workers.getMasterID();
  }

  public boolean isMaster() {
    return this.workers.isMaster();
  }

  public int getNumWorkers() {
    return this.workers.getNumWorkers();
  }

  public int getMinID() {
    return this.workers.getMinID();
  }

  public int getMaxID() {
    return this.workers.getMaxID();
  }

  public boolean barrier(String contextName,
    String operationName) {
    boolean isSuccess =
      Communication.barrier(contextName,
        operationName, dataMap, workers,
        resourcePool);
    dataMap.cleanOperationData(contextName,
      operationName, resourcePool);
    return isSuccess;
  }

  public <P extends Partition> boolean broadcast(
    String contextName, String operationName,
    Table<P> table, int bcastWorkerID,
    boolean useMSTBcast) {
    boolean isSucess =
      BcastCollective.broadcast(contextName,
        operationName, table, bcastWorkerID,
        useMSTBcast, dataMap, workers,
        resourcePool);
    dataMap.cleanOperationData(contextName,
      operationName, resourcePool);
    return isSucess;
  }

  public <P extends Partition> boolean broadcast(
    String contextName, String operationName,
    Table<P> table, int bcastWorkerID) {
    return broadcast(contextName, operationName,
      table, bcastWorkerID, true);
  }

  public <P extends Partition> boolean
    broadcastLarge(String contextName,
      String operationName, Table<P> table,
      int bcastWorkerID) {
    boolean isSucess =
      BcastCollective.broadcastLarge(contextName,
        operationName, table, bcastWorkerID,
        dataMap, workers, resourcePool);
    dataMap.cleanOperationData(contextName,
      operationName, resourcePool);
    return isSucess;
  }

  public <P extends Partition> boolean
    broadcastLarge(String contextName,
      String operationName, Table<P> table,
      int partitionCount, int bcastWorkerID) {
    boolean isSucess =
      BcastCollective.broadcastLarge(contextName,
        operationName, table, partitionCount,
        bcastWorkerID, dataMap, workers,
        resourcePool);
    dataMap.cleanOperationData(contextName,
      operationName, resourcePool);
    return isSucess;
  }

  public <P extends Partition> boolean reduce(
    String contextName, String operationName,
    Table<P> table, int reduceWorkerID) {
    boolean isSuccess =
      ReduceCollective.reduce(contextName,
        operationName, table, reduceWorkerID,
        dataMap, workers, resourcePool);
    dataMap.cleanOperationData(contextName,
      operationName, resourcePool);
    return isSuccess;
  }

  public <P extends Partition> boolean allgather(
    String contextName, String operationName,
    Table<P> table) {
    boolean isSuccess =
      AllgatherCollective.allgather(contextName,
        operationName, table, dataMap, workers,
        resourcePool, Constants.NUM_THREADS);
    dataMap.cleanOperationData(contextName,
      operationName, resourcePool);
    return isSuccess;
  }

  public <P extends Partition> boolean allreduce(
    String contextName, String operationName,
    Table<P> table) {
    boolean isSuccess =
      AllreduceCollective.allreduce(contextName,
        operationName, table, dataMap, workers,
        resourcePool);
    dataMap.cleanOperationData(contextName,
      operationName, resourcePool);
    return isSuccess;
  }

  public
    <P extends Partition, PF extends PartitionFunction<P>>
    boolean allreduceLarge(String contextName,
      String operationName, Table<P> table,
      PF function) throws Exception {
    boolean isSucess =
      AllreduceCollective.allreduceLarge(
        contextName, operationName, table,
        function, dataMap, workers, resourcePool);
    return isSucess;
  }

  public <P extends Partition> boolean regroup(
    String contextName, String operationName,
    Table<P> table) {
    boolean isSucess =
      RegroupCollective.regroupCombine(
        contextName, operationName, table,
        dataMap, workers, resourcePool);
    dataMap.cleanOperationData(contextName,
      operationName, resourcePool);
    return isSucess;
  }

  public <P extends Partition> boolean
    regroupLarge(String contextName,
      String operationName, Table<P> table,
      int numTasks) {
    boolean isSucess =
      RegroupCollective.regroupCombineLarge(
        contextName, operationName, table,
        numTasks, dataMap, workers, resourcePool);
    dataMap.cleanOperationData(contextName,
      operationName, resourcePool);
    return isSucess;
  }

  public
    <P1 extends Partition, P2 extends Partition>
    boolean graphScatter(String contextName,
      String operationName,
      Table<P1> dynamicTable,
      Table<P2> staticTable) {
    boolean isSuccess =
      GraphCollective.allToAll(contextName,
        operationName, dynamicTable, staticTable,
        dataMap, workers, resourcePool);
    dataMap.cleanOperationData(contextName,
      operationName, resourcePool);
    return isSuccess;
  }

  public
    <P1 extends Partition, P2 extends Partition, PF extends PartitionFunction<P1>>
    boolean graphReduceScatter(
      String contextName, String operationName,
      Table<P1> dynamicTable,
      Table<P2> staticTable, PF function) {
    boolean isSuccess =
      GraphCollective.allToOneToAll(contextName,
        operationName, dynamicTable, staticTable,
        function, dataMap, workers, resourcePool);
    dataMap.cleanOperationData(contextName,
      operationName, resourcePool);
    return isSuccess;
  }

  public
    <P extends Partition, PF extends PartitionFunction<P>>
    boolean graphReduceScatter(
      String contextName, String operationName,
      Table<P> table, PF function) {
    boolean isSuccess =
      GraphCollective.allToOneToAll(contextName,
        operationName, table, function, dataMap,
        workers, resourcePool);
    dataMap.cleanOperationData(contextName,
      operationName, resourcePool);
    return isSuccess;
  }

  public <P extends Partition> boolean
    syncLocalWithGlobal(String contextName,
      String operationName, Table<P> localTable,
      Table<P> globalTable, boolean useBcast,
      int numTasks) {
    boolean isSuccess =
      LocalGlobalSyncCollective
        .syncLocalWithGlobal(contextName,
          operationName, localTable, globalTable,
          useBcast, numTasks, dataMap, workers,
          resourcePool);
    dataMap.cleanOperationData(contextName,
      operationName, resourcePool);
    return isSuccess;
  }

  public <P extends Partition> boolean
    syncGlobalWithLocal(String contextName,
      String operationName, Table<P> localTable,
      Table<P> globalTable, boolean useReduce,
      int numTasks) {
    boolean isSuccess =
      LocalGlobalSyncCollective
        .syncGlobalWithLocal(contextName,
          operationName, localTable, globalTable,
          useReduce, numTasks, dataMap, workers,
          resourcePool);
    dataMap.cleanOperationData(contextName,
      operationName, resourcePool);
    return isSuccess;
  }

  public <P extends Partition> boolean
    rotateGlobal(String contextName,
      String operationName, Table<P> globalTable) {
    boolean isSuccess =
      LocalGlobalSyncCollective.rotateGlobal(
        contextName, operationName, globalTable,
        dataMap, workers, resourcePool);
    dataMap.cleanOperationData(contextName,
      operationName, resourcePool);
    return isSuccess;
  }

  public <P extends Partition> boolean
    rotateGlobal(String contextName,
      String operationName, Table<P> globalTable,
      Int2IntMap rotateMap) {
    boolean isSuccess =
      LocalGlobalSyncCollective
        .rotateGlobal(contextName, operationName,
          globalTable, rotateMap, dataMap,
          workers, resourcePool);
    dataMap.cleanOperationData(contextName,
      operationName, resourcePool);
    return isSuccess;
  }

  public Event getEvent() {
    return eventQueue.getEvent();
  }

  public Event waitEvent() {
    return eventQueue.waitEvent();
  }

  public boolean addLocalEvent(Event event) {
    if (event.getEventType() == EventType.LOCAL_EVENT
      && event.getBody() != null) {
      eventQueue.addEvent(event);
      return true;
    } else {
      return false;
    }
  }

  public void sendMessageEvent(int destWorkerID,
    Event event) {
    client.submitMessageEvent(destWorkerID,
      event, false);
  }

  /**
   * Broadcast an event to all workers other than
   * the sender.
   * 
   * @param event
   */
  public void sendCollectiveEvent(Event event) {
    client.submitCollectiveEvent(event, false);
  }

  public EventFuture sendMessageEvent(
    int destWorkerID, Event event,
    boolean hasFuture) {
    return client.submitMessageEvent(
      destWorkerID, event, hasFuture);
  }

  public EventFuture sendCollectiveEvent(
    Event event, boolean hasFuture) {
    return client.submitCollectiveEvent(event,
      hasFuture);
  }

  public ResourcePool getResourcePool() {
    return this.resourcePool;
  }

  public <P extends Partition> void
    releasePartition(P partition) {
    PartitionUtils.releasePartition(resourcePool,
      partition);
  }

  public <P extends Partition> void releaseTable(
    Table<P> table) {
    PartitionUtils.releaseTable(resourcePool,
      table);
  }

  protected void logMemUsage() {
    LOG.info("Total Memory (bytes): " + " "
      + Runtime.getRuntime().totalMemory()
      + ", Free Memory (bytes): "
      + Runtime.getRuntime().freeMemory());
  }

  protected void logGCTime() {
    long totalGarbageCollections = 0;
    long garbageCollectionTime = 0;
    for (GarbageCollectorMXBean gc : ManagementFactory
      .getGarbageCollectorMXBeans()) {
      long count = gc.getCollectionCount();
      if (count >= 0) {
        totalGarbageCollections += count;
      }
      long time = gc.getCollectionTime();
      if (time >= 0) {
        garbageCollectionTime += time;
      }
    }
    LOG.info("Total Garbage Collections: "
      + totalGarbageCollections
      + ", Total Garbage Collection Time (ms): "
      + garbageCollectionTime);
  }

  /**
   * Called once at the beginning of the task.
   */
  protected void setup(Context context)
    throws IOException, InterruptedException {
    // NOTHING
  }

  protected void mapCollective(
    KeyValReader reader, Context context)
    throws IOException, InterruptedException {
    while (reader.nextKeyValue()) {
      // Do...
    }
  }

  /**
   * Called once at the end of the task.
   */
  protected void cleanup(Context context)
    throws IOException, InterruptedException {
    // NOTHING
  }

  /**
   * Expert users can override this method for
   * more complete control over the execution of
   * the Mapper.
   * 
   * @param context
   * @throws IOException
   */
  public void run(Context context)
    throws IOException, InterruptedException {
    long time1 = System.currentTimeMillis();
    boolean success =
      initCollCommComponents(context);
    long time2 = System.currentTimeMillis();
    LOG.info("Initialize Harp components (ms): "
      + (time2 - time1));
    if (!success) {
      if (client != null) {
        client.stop();
      }
      // Stop the server
      if (server != null) {
        server.stop();
      }
      throw new IOException(
        "Fail to do master barrier.");
    }
    setup(context);
    KeyValReader reader =
      new KeyValReader(context);
    try {
      mapCollective(reader, context);
      resourcePool.logResourcePoolUsage();
    } catch (Throwable t) {
      LOG.error("Fail to do map-collective.", t);
      throw new IOException(t);
    } finally {
      cleanup(context);
      ConnectionPool.closeAllConncetions();
      client.stop();
      server.stop();
    }
  }
}