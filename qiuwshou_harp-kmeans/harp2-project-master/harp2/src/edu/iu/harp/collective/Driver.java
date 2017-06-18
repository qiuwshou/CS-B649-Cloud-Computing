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

import java.io.File;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import edu.iu.harp.client.DataSender;
import edu.iu.harp.client.Sender;
import edu.iu.harp.depl.CMDOutput;
import edu.iu.harp.depl.QuickDeployment;
import edu.iu.harp.io.ConnectionPool;
import edu.iu.harp.io.Constants;
import edu.iu.harp.io.Data;
import edu.iu.harp.io.DataMap;
import edu.iu.harp.io.EventQueue;
import edu.iu.harp.io.DataType;
import edu.iu.harp.io.IOUtils;
import edu.iu.harp.message.Ack;
import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.server.reuse.Server;
import edu.iu.harp.worker.WorkerInfo;
import edu.iu.harp.worker.Workers;
import edu.iu.harp.worker.Workers.WorkerInfoList;

public class Driver {
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(Driver.class);
  private static final String bcast_script =
    QuickDeployment.getProjectHomePath()
      + QuickDeployment.getBinDirectory()
      + "collective/bcast.sh";
  private static final String regroup_script =
    QuickDeployment.getProjectHomePath()
      + QuickDeployment.getBinDirectory()
      + "collective/regroup.sh";
  private static final String wordcount_script =
    QuickDeployment.getProjectHomePath()
      + QuickDeployment.getBinDirectory()
      + "collective/wordcount.sh";
  private static final String allgather_script =
    QuickDeployment.getProjectHomePath()
      + QuickDeployment.getBinDirectory()
      + "collective/allgather.sh";
  private static final String allreduce_script =
    QuickDeployment.getProjectHomePath()
      + QuickDeployment.getBinDirectory()
      + "collective/allreduce.sh";
  private static final String reduce_script =
    QuickDeployment.getProjectHomePath()
      + QuickDeployment.getBinDirectory()
      + "collective/reduce.sh";
  private static final String graph_script =
    QuickDeployment.getProjectHomePath()
      + QuickDeployment.getBinDirectory()
      + "collective/graph.sh";
  private static final String event_script =
    QuickDeployment.getProjectHomePath()
      + QuickDeployment.getBinDirectory()
      + "collective/event.sh";

  public static void main(String args[])
    throws Exception {
    // Get driver host and port
    String driverHost = args[0];
    int driverPort = Integer.parseInt(args[1]);
    String task = args[2];
    long jobID = 0;
    // Initialize
    LOG.info("Initialize driver.");
    EventQueue eventQueue = new EventQueue();
    DataMap dataMap = new DataMap();
    Workers workers = new Workers();
    ResourcePool resourcePool =
      new ResourcePool();
    Server server =
      new Server(driverHost, driverPort,
        Constants.NUM_RECV_THREADS,
        eventQueue, dataMap, workers,
        resourcePool);
    server.start();
    // Start workers
    LOG.info("Start all workers...");
    LOG.info("Number of workers: "
      + workers.getNumWorkers());
    boolean isRunning = false;
    if (task.equals("bcast")) {
      // args[3]: totalByteData
      // args[4]: numLoops
      isRunning =
        startAllWorkers(workers, resourcePool,
          bcast_script, driverHost, driverPort,
          jobID, args[3], args[4]);
    } else if (task.equals("regroup")) {
      // args[3]: partitionByteData
      // args[4]: numPartitions
      isRunning =
        startAllWorkers(workers, resourcePool,
          regroup_script, driverHost, driverPort,
          jobID, args[3], args[4]);
    } else if (task.equals("wordcount")) {
      isRunning =
        startAllWorkers(workers, resourcePool,
          wordcount_script, driverHost,
          driverPort, jobID);
    } else if (task.equals("allgather")) {
      isRunning =
        startAllWorkers(workers, resourcePool,
          allgather_script, driverHost,
          driverPort, jobID, args[3], args[4]);
    } else if (task.equals("allreduce")) {
      isRunning =
        startAllWorkers(workers, resourcePool,
          allreduce_script, driverHost,
          driverPort, jobID, args[3], args[4]);
    } else if (task.equals("reduce")) {
      isRunning =
        startAllWorkers(workers, resourcePool,
          reduce_script, driverHost, driverPort,
          jobID, args[3], args[4]);
    } else if (task.equals("graph")) {
      // args[3]: iteration
      isRunning =
        startAllWorkers(workers, resourcePool,
          graph_script, driverHost, driverPort,
          jobID, args[3]);
    } else if (task.equals("event")) {
      isRunning =
        startAllWorkers(workers, resourcePool,
          event_script, driverHost, driverPort,
          jobID);
    } else {
      LOG.info("Inccorect task command... ");
    }
    if (isRunning) {
      waitForAllWorkers(jobID + "",
        "report-to-driver", dataMap, workers,
        resourcePool);
    }
    ConnectionPool.closeAllConncetions();
    server.stop();
    System.exit(0);
  }

  /**
   * It seems that the logger you get in static
   * field can be re-initialized later.
   * 
   * @param workerID
   */
  static void initLogger(int workerID) {
    String fileName =
      "harp-worker-" + workerID + ".log";
    File file = new File(fileName);
    if (file.exists()) {
      file.delete();
    }
    FileAppender fileAppender =
      new FileAppender();
    fileAppender.setName("FileLogger");
    fileAppender.setFile(fileName);
    fileAppender
      .setLayout(new PatternLayout(
        "%d{dd MMM yyyy HH:mm:ss,SSS} %-4r [%t] %-5p %c %x - %m%n"));
    fileAppender.setAppend(true);
    fileAppender.activateOptions();
    // Add appender to any Logger (here is root)
    Logger.getRootLogger().addAppender(
      fileAppender);
  }

  private static boolean startAllWorkers(
    Workers workers, ResourcePool resourcePool,
    String script, String driverHost,
    int driverPort, long jobID,
    String... otherArgs) {
    boolean isSuccess = true;
    String workerNode;
    int workerID;
    WorkerInfoList workerInfoList =
      workers.getWorkerInfoList();
    for (WorkerInfo workerInfo : workerInfoList) {
      workerNode = workerInfo.getNode();
      workerID = workerInfo.getID();
      isSuccess =
        startWorker(workerNode, script,
          driverHost, driverPort, workerID,
          jobID, otherArgs);
      LOG.info("Start worker ID: "
        + workerInfo.getID() + " Node: "
        + workerInfo.getNode() + " Result: "
        + isSuccess);
      if (!isSuccess) {
        return false;
      }
    }
    return true;
  }

  private static boolean
    startWorker(String workerHost, String script,
      String driverHost, int driverPort,
      int workerID, long jobID,
      String... otherArgs) {
    int otherArgsLen = otherArgs.length;
    String cmdstr[] =
      new String[8 + otherArgsLen];
    cmdstr[0] = "ssh";
    cmdstr[1] = workerHost;
    cmdstr[2] = script;
    cmdstr[3] = driverHost;
    cmdstr[4] = driverPort + "";
    cmdstr[5] = workerID + "";
    cmdstr[6] = jobID + "";
    for (int i = 0; i < otherArgsLen; i++) {
      cmdstr[7 + i] = otherArgs[i];
    }
    cmdstr[cmdstr.length - 1] = "&";
    CMDOutput cmdOutput =
      QuickDeployment.executeCMDandNoWait(cmdstr);
    return cmdOutput.getExecutionStatus();
  }

  private static boolean waitForAllWorkers(
    String contextName, String opName,
    DataMap dataMap, Workers workers,
    ResourcePool resourcePool) {
    int count = 0;
    while (count < workers.getNumWorkers()) {
      Data data =
        IOUtils.waitAndGet(dataMap, contextName,
          opName);
      if (data != null) {
        LOG.info("Worker Status: "
          + data.getWorkerID());
        data.release(resourcePool);
        data = null;
        count++;
      } else {
        return false;
      }
    }
    return true;
  }

  /**
   * Report to driver that worker
   */
  public static boolean reportToDriver(
    String contextName, String operationName,
    int workerID, String driverHost,
    int driverPort, ResourcePool resourcePool) {
    LOG.info("Worker " + workerID + " reports. ");
    // Send ack to report worker status
    Ack ack =
      (Ack) resourcePool
        .getWritableObject(Ack.class);
    Data ackData =
      new Data(DataType.STRUCT_OBJECT,
        contextName, workerID, operationName, ack);
    Sender sender =
      new DataSender(ackData, driverHost,
        driverPort, resourcePool,
        Constants.SEND_DECODE);
    boolean isSuccess = sender.execute();
    ackData.release(resourcePool);
    ackData = null;
    ack = null;
    return isSuccess;
  }
}
