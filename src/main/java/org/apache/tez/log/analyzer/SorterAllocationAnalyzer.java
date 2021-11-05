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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SorterAllocationAnalyzer extends BaseAnalyzer {

  private final Pattern sortedAllocationPattern =
      Pattern.compile("Setting up PipelinedSorter for (.*): , UsingHashComparator=(.*), "
          + "maxMemUsage=(.*), lazyAllocateMem=(.*), minBlockSize=(.*), initial BLOCK_SIZE=(.*), "
          + "finalMergeEnabled=(.*), pipelinedShuffle=(.*), sendEmptyPartitions=(.*), tez"
          + ".runtime.io.sort.mb=(.*)");

  private final Pattern taskAttemptPattern = Pattern.compile("Initializing task, "
      + "taskAttemptId=(.*)");

  private final Map<String, SortAllocationDetails>
      sortAllocationMap = Maps.newTreeMap();

  String taskAttemptId;

  @Override
  public void process(String line) throws IOException {
    if (line.contains("Initializing task, taskAttemptId=")) {
      Matcher matcher = taskAttemptPattern.matcher(line);
      while (matcher.find()) {
        taskAttemptId = matcher.group(1);
        return;
      }
    }
    if (line.contains("Setting up PipelinedSorter")) {
      Matcher matcher = sortedAllocationPattern.matcher(line);
      while (matcher.find()) {
        String vertexName = matcher.group(1);
        String maxMemUsage = matcher.group(3);
        String lazyMem = matcher.group(4);
        String minBlockSize = matcher.group(5);
        String blockSize = matcher.group(6);
        String finalMergeEnabled = matcher.group(7);
        String pipelinedShuffle = matcher.group(8);
        String sendEmptyPartitions = matcher.group(9);
        String sortMb = matcher.group(10);
        SortAllocationDetails
            sortAllocationDetails =
            new SortAllocationDetails(taskAttemptId, vertexName, maxMemUsage, lazyMem,
                minBlockSize, blockSize, finalMergeEnabled, pipelinedShuffle, sendEmptyPartitions,
                sortMb, getCurrentLineNumber());
        sortAllocationMap.put(taskAttemptId, sortAllocationDetails);
      }
    }
  }

  @Override
  public String getAnalysis() throws IOException {
    TreeSet<SortAllocationDetails>
        finishedSet =
        new TreeSet<>(new Comparator<SortAllocationDetails>() {
          @Override public int compare(SortAllocationDetails o1,
              SortAllocationDetails o2) {
            //reverse order
            return o2.attempt.compareTo(o1.attempt);
          }
        });
    finishedSet.addAll(sortAllocationMap.values());

    Set<String> basisAnalysis = new HashSet<String>();
    for(SortAllocationDetails allocationDetails : finishedSet) {
      if (allocationDetails.lazyMem.trim().equalsIgnoreCase("false")) {
        basisAnalysis.add(allocationDetails.attempt + ". LazyMem is false. Set tez.runtime"
            + ".pipelined"
            + ".sorter.lazy-allocate"
            + ".memory=true; \n");
      }
/*
     if (allocationDetails.sortMb != null && Integer.getInteger(allocationDetails.sortMb.trim()) < 256) {
        basisAnalysis.add(allocationDetails.attempt + ". SortMB seems small at " + allocationDetails.sortMb.trim() + "\n");
      }
*/
    }
    return basisAnalysis.toString() + Joiner.on('\n').join(finishedSet);
  }

  @Override
  public String getName() {
      return "Sort Allocation Analyzer";
  }

  @Override
  public Object getResult() {
    return Collections.unmodifiableMap(sortAllocationMap);
  }

  public static final class SortAllocationDetails {
    public final String attempt;
    public final String vertexName;
    public final String maxMemUsage;
    public final String lazyMem;
    public final String minBlockSize;
    public final String blockSize;
    public final String finalMergeEnabled;
    public final String pipelinedShuffle;
    public final String sendEmptyPartitions;
    public final String sortMb;
    public final long lineNumber;

    public SortAllocationDetails(String attempt, String vertexName, String maxMemUsage,
        String lazyMem,
        String minBlockSize, String blockSize, String finalMergeEnabled, String pipelinedShuffle,
        String sendEmptyPartitions, String sortMb, long currentLineNumber) {
      this.attempt = attempt;
      this.vertexName = vertexName;
      this.maxMemUsage = maxMemUsage;
      this.lazyMem = lazyMem;
      this.minBlockSize = minBlockSize;
      this.blockSize = blockSize;
      this.finalMergeEnabled = finalMergeEnabled;
      this.pipelinedShuffle = pipelinedShuffle;
      this.sendEmptyPartitions = sendEmptyPartitions;
      this.sortMb = sortMb;
      this.lineNumber = currentLineNumber;
    }

    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("line", lineNumber)
          .add("attempt", attempt)
          .add("vertexName", vertexName)
          .add("maxMemUsage", maxMemUsage)
          .add("lazyMem", lazyMem)
          .add("minBlockSize", minBlockSize)
          .add("blockSize", blockSize)
          .add("finalMergeEnabled", finalMergeEnabled)
          .add("pipelinedShuffle", pipelinedShuffle)
          .add("sendEmptyPartitions", sendEmptyPartitions)
          .add("sortMb", sortMb)
          .toString();
    }
  }
}
