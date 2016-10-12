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
      Pattern.compile("vertexName=(.*), taskAttemptId=(.*), creationTime(.*), timeTaken=(\\d+), status=(.*), errorEnum=(.*), diagnostics=(.*)");

  private final Map<String, TaskAttemptFinished> taskAttemptFinishedMap = Maps.newTreeMap();

  @Override
  public void process(String line) throws IOException {
    if (line.contains("TASK_ATTEMPT_FINISHED")) {
      Matcher matcher = taskAttemptFinishedPattern.matcher(line);
      while (matcher.find()) {
        String vertexId = matcher.group(1);
        String taskAttemptId = matcher.group(2);
        String timeTaken = matcher.group(4);
        String status = matcher.group(5);
        String error = matcher.group(6);
        String diagnostics = matcher.group(7);
        TaskAttemptFinished
            taskAttemptFinished =
            new TaskAttemptFinished(vertexId, taskAttemptId, timeTaken, status, error, diagnostics,
                getCurrentLineNumber());
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
    private final String vertexId;
    private final String taskAttemptId;
    private final String timeTaken;
    private final String status;
    private final String error;
    private final String diagnostics;
    private final long lineNumber;

    TaskAttemptFinished(String vertexId, String taskAttemptId, String timeTaken, String status,
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
