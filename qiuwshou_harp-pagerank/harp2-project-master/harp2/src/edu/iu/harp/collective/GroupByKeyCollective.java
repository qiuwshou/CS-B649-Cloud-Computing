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

package edu.iu.harp.collective;

import java.util.LinkedList;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import edu.iu.harp.comm.Communication;
import edu.iu.harp.io.ConnectionPool;
import edu.iu.harp.io.Constants;
import edu.iu.harp.io.DataMap;
import edu.iu.harp.io.EventQueue;
import edu.iu.harp.keyval.DoubleCombiner;
import edu.iu.harp.keyval.DoubleVal;
import edu.iu.harp.keyval.IntAvg;
import edu.iu.harp.keyval.IntCountVal;
import edu.iu.harp.keyval.IntCountValPlus;
import edu.iu.harp.keyval.Key;
import edu.iu.harp.keyval.KeyValPartition;
import edu.iu.harp.keyval.KeyValTable;
import edu.iu.harp.keyval.StringKey;
import edu.iu.harp.keyval.ValFunction;
import edu.iu.harp.keyval.Value;
import edu.iu.harp.partition.PartitionUtils;
import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.server.reuse.Server;
import edu.iu.harp.worker.Workers;

public class GroupByKeyCollective {

  /** Class logger */
  protected static final Logger LOG = Logger
    .getLogger(GroupByKeyCollective.class);

  public static void main(String args[])
    throws Exception {
    String driverHost = args[0];
    int driverPort = Integer.parseInt(args[1]);
    int workerID = Integer.parseInt(args[2]);
    long jobID = Long.parseLong(args[3]);
    // Initialize log
    Driver.initLogger(workerID);
    LOG
      .info("args[] " + driverHost + " "
        + driverPort + " " + workerID + " "
        + jobID);
    // ---------------------------------------------------
    // Worker initialize
    EventQueue eventQueue = new EventQueue();
    DataMap dataMap = new DataMap();
    Workers workers = new Workers(workerID);
    ResourcePool resourcePool =
      new ResourcePool();
    Server server =
      new Server(workers.getSelfInfo().getNode(),
        workers.getSelfInfo().getPort(),
        Constants.NUM_RECV_THREADS, eventQueue,
        dataMap, workers, resourcePool);
    server.start();
    String contextName = jobID + "";
    // Barrier guarantees the living workers get
    // the same view of the barrier result
    LOG.info("Start Barrier");
    boolean isSuccess =
      Communication
        .barrier(contextName, "barrier", dataMap,
          workers, resourcePool);
    LOG.info("Barrier: " + isSuccess);
    // -----------------------------------------------
    // Generate words into wordcount table
    KeyValTable<StringKey, IntCountVal> table =
      new KeyValTable<>(StringKey.class,
        IntCountVal.class, new IntCountValPlus(),
        10, resourcePool);
    try {
      // Don't create StringKey through the
      // resource pool
      table.addKeyVal(new StringKey("Facebook"),
        new IntCountVal(1, 8));
      table.addKeyVal(new StringKey("Google"),
        new IntCountVal(1, 9));
      table.addKeyVal(new StringKey("Amazon"),
        new IntCountVal(1, 6));
      table.addKeyVal(new StringKey("LinkedIn"),
        new IntCountVal(1, 7));
      table.addKeyVal(new StringKey("Twitter"),
        new IntCountVal(1, 7));
      table.addKeyVal(new StringKey("Microsoft"),
        new IntCountVal(1, 9));
      LOG.info("Log key-valyue table.");
      for (KeyValPartition partition : table
        .getPartitions()) {
        for (Entry<Key, Value> entry : partition
          .getKeyVals()) {
          LOG.info(((StringKey) entry.getKey())
            .getStringKey()
            + " "
            + ((IntCountVal) entry.getValue())
              .getIntValue());
        }
      }
    } catch (Exception e) {
      LOG.error(
        "Error in generating word and count.", e);
    }
    LOG.info("Word and count are generated.");
    // --------------------------------------------------------
    // The partition seed of the new table
    // is not required to be the same with the
    // old table
    KeyValTable<StringKey, DoubleVal> newTable =
      new KeyValTable<StringKey, DoubleVal>(
        StringKey.class, DoubleVal.class,
        new DoubleCombiner(), 10, resourcePool);
    groupByAggregate(contextName,
      "group-by-key-aggregate", table, newTable,
      new IntAvg(), dataMap, workers,
      resourcePool);
    // Print new table
    for (KeyValPartition partition : newTable
      .getPartitions()) {
      for (Entry entry : partition.getKeyVals()) {
        LOG.info(((StringKey) entry.getKey())
          .getStringKey()
          + " "
          + ((DoubleVal) entry.getValue())
            .getDoubleValue());
      }
    }
    // -------------------------------------------------------
    Driver.reportToDriver(contextName,
      "report-to-driver", workers.getSelfID(),
      driverHost, driverPort, resourcePool);
    ConnectionPool.closeAllConncetions();
    server.stop();
    System.exit(0);
  }

  private static
    <K extends Key, V1 extends Value, V2 extends Value, VF extends ValFunction<V1, V2>>
    void
    applyValFunction(KeyValTable<K, V1> oldTable,
      KeyValTable<K, V2> newTable, VF valFunction) {
    LinkedList<Key> rmKeys = new LinkedList<>();
    for (KeyValPartition partition : oldTable
      .getPartitions()) {
      for (Entry<Key, Value> entry : partition
        .getKeyVals()) {
        K key = (K) entry.getKey();
        V1 value = (V1) entry.getValue();
        newTable.addKeyVal((K) key,
          valFunction.apply(value));
        rmKeys.add(key);
      }
      for (Key key : rmKeys) {
        partition.removeVal(key);
      }
      rmKeys.clear();
    }
  }

  public static
    <K extends Key, V1 extends Value, V2 extends Value, VF extends ValFunction<V1, V2>>
    void groupByAggregate(String contextName,
      String operationName,
      KeyValTable<K, V1> oldTable,
      KeyValTable<K, V2> newTable,
      VF valFunction, DataMap dataMap,
      Workers workers, ResourcePool resourcePool) {
    RegroupCollective.regroupCombine(contextName,
      operationName, oldTable, dataMap, workers,
      resourcePool);
    applyValFunction(oldTable, newTable,
      valFunction);
    PartitionUtils.releaseTable(resourcePool,
      oldTable);
  }
}
