package edu.iu.harp.server.reuse;

import edu.iu.harp.event.EventType;
import edu.iu.harp.io.Data;
import edu.iu.harp.io.DataMap;
import edu.iu.harp.io.DataUtils;
import edu.iu.harp.io.EventQueue;
import edu.iu.harp.resource.ResourcePool;

public class Decoder implements Runnable {

  private final Data data;
  private final ResourcePool pool;
  private final EventQueue eventQueue;
  private final DataMap dataMap;

  public Decoder(Data data, ResourcePool pool,
    EventQueue eventQueue, DataMap dataMap) {
    this.data = data;
    this.pool = pool;
    this.eventQueue = eventQueue;
    this.dataMap = dataMap;
  }

  @Override
  public void run() {
    // Decode data array
    data.decodeBodyArray(pool);
    data.releaseBodyArray(pool);
    // If the data is not for operation,
    // put it to the queue with message event
    // type
    // If any exception happens in receiving,
    // It throws
    // LOG.info("Start adding.");
    DataUtils.addDataToQueueOrMap(eventQueue,
      EventType.MESSAGE_EVENT, dataMap, data);
  }
}
