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
 * Print where the task attempts are started
 */
public class TaskAttemptFinishedAnalyzer extends BaseAnalyzer {

  private final Pattern taskAttemptFinishedPattern =
      Pattern.compile("vertexName=(.*), taskAttemptId=(.*), creationTime(.*), "
          + "startTime=(.*), finishTime=(.*), timeTaken=(\\d+), status=(.*),"
          + " errorEnum=(.*), diagnostics=(.*)");

  private final Map<String, TaskAttemptFinished> taskAttemptFinishedMap = Maps.newTreeMap();

  @Override
  public void process(String line) throws IOException {
    if (line.contains("TASK_ATTEMPT_FINISHED")) {
      Matcher matcher = taskAttemptFinishedPattern.matcher(line);
      while (matcher.find()) {
        String vertexId = matcher.group(1);
        String taskAttemptId = matcher.group(2);
        String startTime = matcher.group(4);
        String finishTime = matcher.group(5);
        String timeTaken = matcher.group(6);
        String status = matcher.group(7);
        String error = matcher.group(8);
        String diagnostics = matcher.group(9);
        TaskAttemptFinished
            taskAttemptFinished =
            new TaskAttemptFinished(vertexId, taskAttemptId, startTime, finishTime, timeTaken,
                status, error, diagnostics, getCurrentLineNumber());
        taskAttemptFinishedMap.put(taskAttemptId, taskAttemptFinished);
      }
    }
  }

  @Override
  public String getAnalysis() throws IOException {
    Joiner.MapJoiner joiner = Joiner.on('\n').withKeyValueSeparator("=");
    return joiner.join(taskAttemptFinishedMap);
  }

  @Override
  public String getName() {
    return "Task Attempt Finished";
  }

  @Override
  public Object getResult() {
    return Collections.unmodifiableMap(taskAttemptFinishedMap);
  }

  public static final class TaskAttemptFinished {
    public final String vertexId;
    public final String taskAttemptId;
    public final String startTime;
    public final String finishTime;
    public final String timeTaken;
    public final String status;
    public final String error;
    public final String diagnostics;
    public final long lineNumber;

    TaskAttemptFinished(String vertexId, String taskAttemptId, String startTime, String
        finishTime, String timeTaken, String status,
        String error, String diagnostics, long lineNumber) {
      this.vertexId = vertexId;
      this.taskAttemptId = taskAttemptId;
      this.startTime = startTime;
      this.finishTime = finishTime;
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
          .add("startTime", startTime)
          .add("finishTime", finishTime)
          .add("timeTaken", timeTaken)
          .add("status", status)
          .add("error", error)
          .add("diagnostics", diagnostics)
          .toString();
    }
  }
}
