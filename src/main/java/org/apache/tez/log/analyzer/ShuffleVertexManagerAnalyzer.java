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

import org.apache.tez.log.IAnalyzer;

import java.io.IOException;

public class ShuffleVertexManagerAnalyzer extends BaseAnalyzer {

  private final StringBuilder sb = new StringBuilder();

  // In case we find some interesting line (e.g OOM, we need to capture the stacktrace). Use this
  // for it.
  private int lineCount = 0;

  @Override
  public void process(String line) throws IOException {
    // blindly add it if lineCount > 0 (e.g capture stacktrace)
    if (lineCount > 0) {
      sb.append(line).append("\n");
      lineCount--;
      return;
    }

    //watch out for keywords
    if (line.contains("ShuffleVertexManager") || //weighted distributor
        line.contains("WeightedScalingMemoryDistributor") || //weighted distributor
        line.contains("JVM.maxFree") || //max JVM size
        line.contains("maxSingleShuffleLimit") || //MergeManager
        line.contains("HybridHashTableContainer") || //HybridHashTableContainer
        line.contains("org.apache.hadoop.hive.ql.exec.MapJoinOperator") //org.apache.hadoop.hive.ql.exec.MapJoinOperator
        ) {
      sb.append("line:").append(getCurrentLineNumber()).append(" ").append(line).append("\n");
    }

    if (line.contains("Initializing task,")) {
      sb.append("\n").append("\n").append(line).append("\n"); //2 line breaks to indicate new task
    }

    if (line.contains("java.lang.OutOfMemoryError")) {
      lineCount = 15;
    }
  }

  @Override
  public String getAnalysis() throws IOException {
    return sb.toString();
  }

  @Override
  public String getName() {
    return "ShuffleVertexManager logs";
  }

  @Override
  public Object getResult() {
    return null;
  }
}
