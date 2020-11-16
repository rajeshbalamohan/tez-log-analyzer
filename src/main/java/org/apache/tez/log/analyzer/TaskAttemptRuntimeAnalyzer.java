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
import java.util.Comparator;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Print where the task attempts are started
 */
public class TaskAttemptRuntimeAnalyzer extends BaseAnalyzer {

  private final Pattern taskAttemptFinishedPattern =
      Pattern.compile("vertexName=(.*), "
          + "taskAttemptId=(.*), creationTime=(.*), "
          + "allocationTime=(.*), startTime=(.*), finishTime=(.*), "
          + "timeTaken=(\\d+), status=(.*), diagnostics=(.*)");

  private final Map<String, TaskAttemptFinished> taskAttemptFinishedMap = Maps.newTreeMap();

  @Override
  public void process(String line) throws IOException {
    if (line.contains("TASK_ATTEMPT_FINISHED")) {
      Matcher matcher = taskAttemptFinishedPattern.matcher(line);
      while (matcher.find()) {
        String vertexId = matcher.group(1);
        String taskAttemptId = matcher.group(2);
        String creationTime = matcher.group(3);
        String allocationTime = matcher.group(4);
        String startTime = matcher.group(5);
        String finishTime = matcher.group(6);
        String timeTaken = matcher.group(7);
        String status = matcher.group(8);
        String diagnostics = matcher.group(9);
        TaskAttemptFinished
            taskAttemptFinished =
            new TaskAttemptFinished(vertexId, taskAttemptId, creationTime, allocationTime,
                startTime, finishTime,
                timeTaken, status, diagnostics, getCurrentLineNumber());
        taskAttemptFinishedMap.put(taskAttemptId, taskAttemptFinished);
      }
    }
  }

  @Override
  public String getAnalysis() throws IOException {
    TreeSet<TaskAttemptFinished> finishedSet = new TreeSet<>(new Comparator<TaskAttemptFinished>() {
      @Override public int compare(TaskAttemptFinished o1, TaskAttemptFinished o2) {
        //reverse order
        return o2.timeTaken.compareTo(o1.timeTaken);
      }
    });
    finishedSet.addAll(taskAttemptFinishedMap.values());
    return Joiner.on('\n').join(finishedSet);
  }

  @Override
  public String getName() {
    return "Task Attempt Finished (Runtime)";
  }

  @Override
  public Object getResult() {
    return Collections.unmodifiableMap(taskAttemptFinishedMap);
  }

  public static final class TaskAttemptFinished {
    public final String vertexId;
    public final String taskAttemptId;
    public final String creationTime;
    public final String allocationTime;
    public final String startTime;
    public final String finishTime;
    public final Long timeTaken;
    public final String status;
    public final String diagnostics;
    public final long lineNumber;

    TaskAttemptFinished(String vertexId, String taskAttemptId, String creationTime,
        String allocationTime, String startTime, String finishTime, String timeTaken, String status,
        String diagnostics, long lineNumber) {
      this.vertexId = vertexId;
      this.taskAttemptId = taskAttemptId;
      this.creationTime = creationTime;
      this.allocationTime = allocationTime;
      this.startTime = startTime;
      this.finishTime = finishTime;
      this.timeTaken = Long.parseLong(timeTaken);
      this.status = status;
      this.diagnostics = diagnostics;
      this.lineNumber = lineNumber;
    }

    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("line", lineNumber)
          .add("vertexId", vertexId)
          .add("taskAttemptId", taskAttemptId)
          .add("creationTime", creationTime)
          .add("allocationTime", allocationTime)
          .add("startTime", startTime)
          .add("finishTime", finishTime)
          .add("timeTaken", timeTaken)
          .add("status", status)
          .toString();
    }

  }
}
