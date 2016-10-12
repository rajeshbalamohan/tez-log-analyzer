package org.apache.tez.log.analyzer;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Maps;

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
public class TaskAttemptKilledAnalyzer extends BaseAnalyzer {

  private final Pattern taskAttemptFinishedPattern =
      Pattern.compile("vertexName=(.*), taskAttemptId=(.*), creationTime(.*), timeTaken=(\\d+), status=(.*), errorEnum=(.*), diagnostics=(.*)");

  private final Map<String, TaskAttemptKilled> taskAttemptsKilledMap = Maps.newTreeMap();

  @Override
  public void process(String line) throws IOException {
    if (line.contains("TASK_ATTEMPT_FINISHED") && line.contains("KILLED")) {
      Matcher matcher = taskAttemptFinishedPattern.matcher(line);
      while (matcher.find()) {
        String vertexId = matcher.group(1);
        String taskAttemptId = matcher.group(2);
        String timeTaken = matcher.group(4);
        String status = matcher.group(5);
        String error = matcher.group(6);
        String diagnostics = matcher.group(7);
        TaskAttemptKilled
            taskAttemptKilled =
            new TaskAttemptKilled(vertexId, taskAttemptId, timeTaken, status, error, diagnostics,
                getCurrentLineNumber());
        taskAttemptsKilledMap.put(taskAttemptId, taskAttemptKilled);
      }
    }
  }

  @Override
  public String getAnalysis() throws IOException {
    Joiner.MapJoiner joiner = Joiner.on('\n').withKeyValueSeparator("=");
    return joiner.join(taskAttemptsKilledMap);
  }

  @Override
  public String getName() {
    return "Task Attempt Killed";
  }

  @Override
  public Object getResult() {
    return Collections.unmodifiableMap(taskAttemptsKilledMap);
  }

  public static final class TaskAttemptKilled {
    private final String vertexId;
    private final String taskAttemptId;
    private final String timeTaken;
    private final String status;
    private final String error;
    private final String diagnostics;
    private final long lineNumber;

    TaskAttemptKilled(String vertexId, String taskAttemptId, String timeTaken, String status,
        String error, String diagnostics, long lineNumber) {
      this.vertexId = vertexId;
      this.taskAttemptId = taskAttemptId;
      this.timeTaken = timeTaken;
      this.status = status;
      this.error = error;
      this.diagnostics = diagnostics;
      this.lineNumber = lineNumber;
    }

    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("line", lineNumber)
          .add("vertexId", vertexId)
          .add("taskAttemptId", taskAttemptId)
          .add("timeTaken", timeTaken)
          .add("status", status)
          .add("error", error)
          .add("diagnostics", diagnostics)
          .toString();
    }
  }
}
