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

package edu.iu.harp.event;

import edu.iu.harp.trans.Transferable;

public class Event {
  private final EventType eventType;
  private final String contextName;
  private final int workerID;
  private final Transferable body;

  public Event(EventType type, String name,
    int sourceID, Transferable b) {
    eventType = type;
    contextName = name;
    workerID = sourceID;
    body = b;
  }

  public EventType getEventType() {
    return eventType;
  }

  public String getContextName() {
    return contextName;
  }

  public int getWorkerID() {
    return workerID;
  }

  public Transferable getBody() {
    return body;
  }
}
