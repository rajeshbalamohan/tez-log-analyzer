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
import org.apache.tez.log.IAnalyzer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VertexKilledAnalyzer extends BaseAnalyzer {

  //Do not reverse. VertexFinished would be unique, not the vertexName..Quite possible that other
  // Dags had same vertex name as well (e.g Map 9 being in dag1 and dag 10)
  private Map<FailedVertex, String> failedVertices = Maps.newLinkedHashMap();
  private static final Pattern pattern =
      Pattern.compile(
          "vertexName=(.*), vertexId=(.*), initRequestedTime=(\\d+), (.*)timeTaken=(\\d+), status=(.*), (.*)numFailedTasks=(\\d+), numSucceededTasks=(\\d+), numKilledTaskAttempts=(\\d+), numKilledTasks=(\\d+), numFailedTaskAttempts=(\\d+), numCompletedTasks=(\\d+)");

  private static final Pattern killedPattern =
      Pattern.compile(
          "vertexName=(.*), vertexId=(.*), "
              + "initRequestedTime=(\\d+), initedTime=(\\d+), "
              + "startRequestedTime=(\\d+), startedTime=(\\d+), "
              + "finishTime=(\\d+), timeTaken=(\\d+), status=(.*), diagnostics=(.*)");

  @Override
  public void process(String line) throws IOException {
    if (line.contains("VERTEX_FINISHED")) {
      Matcher matcher = pattern.matcher(line);
      boolean normalFlow = false;
      while (matcher.find()) {
        normalFlow = true;
        String vertexName = matcher.group(1).trim();
        String vertexID = matcher.group(2).trim();
      }

      if (normalFlow) {
        return;
      }

      //Possible that it is due to failed/killed
      matcher = killedPattern.matcher(line);
      while (matcher.find()) {
        String vertexName = matcher.group(1).trim();
        String vertexID = matcher.group(2).trim();
        long timeTaken = Long.parseLong(matcher.group(8).trim());
        String status = matcher.group(9).trim();
        String diagnostics = matcher.group(10).trim();
        FailedVertex
            failedVertex = new FailedVertex(vertexName, vertexID,
            status, timeTaken, diagnostics);
        failedVertices.put(failedVertex, vertexName);
      }
    }
  }

  @Override
  public String getAnalysis() throws IOException {
    Joiner.MapJoiner joiner = Joiner.on('\n').withKeyValueSeparator("=");
    return joiner.join(failedVertices);
  }

  @Override
  public String getName() {
    return "All Vertices (Succeeded / Killed / Failed)";
  }

  @Override
  public Object getResult() {
    return Collections.unmodifiableMap(failedVertices);
  }

  public static class FailedVertex {
    public final String vertexName;
    public final String vertexId;
    public final String status;
    public final long timeTaken;
    public final String diagnostics;

    FailedVertex(String vertexName, String vertexId, String status,
        long timeTaken, String diagnostics) {
      this.vertexName = vertexName;
      this.vertexId = vertexId;
      this.status = status;
      this.timeTaken = timeTaken;
      this.diagnostics = diagnostics;
    }

    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("vertexName", vertexName)
          .add("vertexId", vertexId)
          .add("status", status)
          .add("timeTaken", timeTaken)
          .add("diagnostics", diagnostics).toString();
    }
  }
}
