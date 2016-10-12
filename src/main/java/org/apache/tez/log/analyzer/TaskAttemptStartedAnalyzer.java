package org.apache.tez.log.analyzer;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Maps;
import org.apache.tez.log.IAnalyzer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Print where the task attempts are started
 */
public class TaskAttemptStartedAnalyzer extends BaseAnalyzer {

  private final Pattern taskAttemptStartedPattern =
      Pattern.compile("vertexName=(.*), taskAttemptId=(.*), "
          + "startTime=(\\d+), containerId=(.*), nodeId=(.*):(\\d+)");

  private final Map<String, TaskAttemptStarted> taskAttemptStartedMap = Maps.newTreeMap();

  @Override
  public void process(String line) throws IOException {
    if (line.contains("TASK_ATTEMPT_STARTED")) {
      Matcher matcher = taskAttemptStartedPattern.matcher(line);
      while (matcher.find()) {
        String vertexId = matcher.group(1);
        String taskAttemptId = matcher.group(2);
        String container = matcher.group(4);
        String node = matcher.group(5);
        TaskAttemptStarted
            taskAttemptStarted =
            new TaskAttemptStarted(vertexId, taskAttemptId, container, node, getCurrentLineNumber());
        taskAttemptStartedMap.put(taskAttemptId, taskAttemptStarted);
      }
    }
  }

  @Override
  public String getAnalysis() throws IOException {
    Joiner.MapJoiner joiner = Joiner.on('\n').withKeyValueSeparator("=");
    return joiner.join(taskAttemptStartedMap);
  }

  @Override
  public String getName() {
    return "Task Attempt Started";
  }

  @Override
  public Object getResult() {
    return Collections.unmodifiableMap(taskAttemptStartedMap);
  }

  public static final class TaskAttemptStarted {
    private final String vertexId;
    private final String taskAttemptId;
    private final String container;
    private final String node;
    private final long lineNumber;

    TaskAttemptStarted(String vertexId, String taskAttemptId, String container, String node,
        long lineNumber) {
      this.vertexId = vertexId;
      this.taskAttemptId = taskAttemptId;
      this.container = container;
      this.node = node;
      this.lineNumber = lineNumber;
    }

    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("line", lineNumber)
          .add("vertexId", vertexId)
          .add("taskAttemptId", taskAttemptId)
          .add("container", container)
          .add("node", node).toString();
    }
  }
}
