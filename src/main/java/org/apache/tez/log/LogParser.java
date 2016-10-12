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

package org.apache.tez.log;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.tez.log.analyzer.ConfigAnalyzer;
import org.apache.tez.log.analyzer.DigraphExtractor;
import org.apache.tez.log.analyzer.FailedTaskAnalyzer;
import org.apache.tez.log.analyzer.HashTableAnalyzer;
import org.apache.tez.log.analyzer.TaskAttemptKilledAnalyzer;
import org.apache.tez.log.analyzer.NodesAnalyzer;
import org.apache.tez.log.analyzer.RackResolverExtractor;
import org.apache.tez.log.analyzer.ShuffleBlamedForAnalyzer;
import org.apache.tez.log.analyzer.ShuffleVertexManagerAnalyzer;
import org.apache.tez.log.analyzer.SplitsAnalyzer;
import org.apache.tez.log.analyzer.StuckTaskAnalyzer;
import org.apache.tez.log.analyzer.TaskAttemptFinishedAnalyzer;
import org.apache.tez.log.analyzer.TaskAttemptStartedAnalyzer;
import org.apache.tez.log.analyzer.VersionInfo;
import org.apache.tez.log.analyzer.VertexFinishedAnalyzer;
import org.apache.tez.log.analyzer.VertexMappingAnalyzer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class LogParser {

  static Map<String, IAnalyzer> standardAnalyzers = Maps.newLinkedHashMap();

  static {
    addStandardAnalyzer(new VersionInfo());
    addStandardAnalyzer(new DigraphExtractor());
    addStandardAnalyzer(new SplitsAnalyzer());
    addStandardAnalyzer(new StuckTaskAnalyzer());
    addStandardAnalyzer(new VertexMappingAnalyzer());
    addStandardAnalyzer(new VertexFinishedAnalyzer());
    addStandardAnalyzer(new ShuffleBlamedForAnalyzer());
    addStandardAnalyzer(new TaskAttemptStartedAnalyzer());
    addStandardAnalyzer(new TaskAttemptFinishedAnalyzer());
    addStandardAnalyzer(new FailedTaskAnalyzer());
    addStandardAnalyzer(new TaskAttemptKilledAnalyzer());
    addStandardAnalyzer(new ShuffleVertexManagerAnalyzer());
    addStandardAnalyzer(new ConfigAnalyzer());
    addStandardAnalyzer(new HashTableAnalyzer());
    addStandardAnalyzer(new FailedTaskAnalyzer());
    addStandardAnalyzer(new NodesAnalyzer());
    addStandardAnalyzer(new RackResolverExtractor());
    // addStandardAnalyzer(new DirectoryInfoExtractor());
    // addStandardAnalyzer(new LaunchContainerInfoExtractor());
  }

  private File file;
  private Map<String, IAnalyzer> analyzers;

  // Any details that needs to be added for analysis
  private List<String> additionalInfo;

  public LogParser(File file) {
    Preconditions.checkArgument(file.exists(), "File " + file + " does not exist");
    this.file = file;
    this.analyzers = Maps.newLinkedHashMap(standardAnalyzers);
    this.additionalInfo = Lists.newLinkedList();
  }

  private static void addStandardAnalyzer(IAnalyzer analyzer) {
    Preconditions.checkArgument(analyzer != null, "Analyzer can't be null");
    standardAnalyzers.put(analyzer.getClass().getName(), analyzer);
  }

  public Map<String, IAnalyzer> getAnalyzers() {
    return Collections.unmodifiableMap(analyzers);
  }

  public void addAnalyzer(IAnalyzer analyzer) {
    Preconditions.checkArgument(analyzer != null, "Analyzer can't be null");
    this.analyzers.put(analyzer.getClass().getName(), analyzer);
  }

  public void process() throws IOException {
    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
      long lineNumber = 0;
      while (reader.ready()) {
        String line = reader.readLine();
        for (IAnalyzer analyzer : analyzers.values()) {
          analyzer.process(line, lineNumber);
        }
        lineNumber++;
      }
    }
  }

  public void writeAnalysis() throws IOException {
    File file = new File(".", "report.txt");
    try (FileWriter writer = new FileWriter(file)) {
      for (IAnalyzer analyzer : analyzers.values()) {
        writer.write(analyzer.getName() + "\n");
        for (int i = 0; i < analyzer.getName().length(); i++) {
          writer.write("*");
        }
        writer.write("\n");
        writer.write(analyzer.getAnalysis() + "\n");
        writer.write("\n");
      }

      // Write additional details if any
      if (additionalInfo.size() > 0) {
        writer.write("Additional Info....\n");
        for (String info : additionalInfo) {
          writer.write("\n");
          writer.write(info);
        }
      }
    }
    System.out.println("Wrote " + file.getAbsolutePath());
  }

  public void addAdditionalAnalysis(String info) {
    additionalInfo.add(info);
  }

  public static void main(String[] args) throws IOException {
    Preconditions.checkArgument(args.length == 1, "Please provide the file to be parsed");
    File inputFile = new File(args[0]);
    Preconditions.checkArgument(inputFile.exists(), "Please provide valid file. " +
        inputFile + " does not exist");

    Stopwatch sw = Stopwatch.createStarted();

    LogParser parser = new LogParser(inputFile);

    parser.process();
    System.out.println();

    IAnalyzer vertexMappingAnalyzer = parser.getAnalyzers()
        .get(VertexMappingAnalyzer.class.getName());
    IAnalyzer vertexFinishedAnalyzer = parser.getAnalyzers()
        .get(VertexFinishedAnalyzer.class.getName());
    if (vertexMappingAnalyzer != null && vertexFinishedAnalyzer != null) {
      System.out.println("Vertices that haven't finished");
      System.out.println("*******************************");
      Map<String, String> vertexMapping = (Map<String, String>) vertexMappingAnalyzer.getResult();
      Map<VertexFinishedAnalyzer.VertexFinished, String> vertexFinishedMap =
          (Map<VertexFinishedAnalyzer.VertexFinished, String>) vertexFinishedAnalyzer.getResult();

      for (Map.Entry<String, String> e : vertexMapping.entrySet()) {
        boolean found = false;
        for (Map.Entry<VertexFinishedAnalyzer.VertexFinished, String> fe : vertexFinishedMap
            .entrySet()) {
          if (fe.getKey().vertexId.equalsIgnoreCase(e.getKey())) {
            found = true;
            break;
          }
        }
        if (!found) {
          System.out.println(e.getKey() + " is not in finished map list. " + e.getValue());
        }
      }
    }

    /**
     * In case shuffle-blamed-for details is there, co-relate with rack details
     */
    IAnalyzer shuffleBlamedFor = parser.getAnalyzers()
        .get(ShuffleBlamedForAnalyzer.class.getName());
    IAnalyzer rackResolver = parser.getAnalyzers()
        .get(RackResolverExtractor.class.getName());
    if (shuffleBlamedFor != null && rackResolver != null) {
      // machine --> rack mapping
      Map<String, String> rackMap = (Map<String, String>) rackResolver.getResult();

      ShuffleBlamedForAnalyzer.ShuffleBlamedForResult result =
          (ShuffleBlamedForAnalyzer.ShuffleBlamedForResult) shuffleBlamedFor.getResult();

      parser.addAdditionalAnalysis("Source machine details..");
      for (String srcMachine : result.getSrcMachines()) {
        //machine:45454, containerPriority= 8, containerResources=<memory:3584, vCores:1>
        String machine = srcMachine.substring(0, srcMachine.indexOf(":"));
        parser.addAdditionalAnalysis(machine  + " --> " + rackMap.get(machine));
      }

      parser.addAdditionalAnalysis("");
      parser.addAdditionalAnalysis("");
      parser.addAdditionalAnalysis("Fetcher machine details..");
      for (String fetcherMachine : result.getFetcherMachines()) {
        //machine:45454, containerPriority= 8, containerResources=<memory:3584, vCores:1>
        String machine = fetcherMachine.substring(0, fetcherMachine.indexOf(":"));
        parser.addAdditionalAnalysis(machine + " --> " + rackMap.get(machine));
      }
    }

    /**
     * Task attempts not finished
     */
    IAnalyzer taskAttemptStarted = parser.getAnalyzers()
        .get(TaskAttemptStartedAnalyzer.class.getName());
    IAnalyzer taskAttemptFinished = parser.getAnalyzers()
        .get(TaskAttemptFinishedAnalyzer.class.getName());
    if (taskAttemptFinished != null && taskAttemptStarted != null) {
      Map<String, TaskAttemptStartedAnalyzer.TaskAttemptStarted> started =
          (Map<String, TaskAttemptStartedAnalyzer.TaskAttemptStarted>) taskAttemptStarted.getResult();
      Map<String, TaskAttemptFinishedAnalyzer.TaskAttemptFinished> finished =
          (Map<String, TaskAttemptFinishedAnalyzer.TaskAttemptFinished>) taskAttemptFinished.getResult();

      parser.addAdditionalAnalysis("List of unfinished tasks!! started=" + started.size() + ", "
          + "finished=" + finished.size());
      for(String task : started.keySet()) {
        //check if this task is in finished keys
        if (!finished.keySet().contains(task)) {
          parser.addAdditionalAnalysis(task + " is not in finished list");
        }
      }
    }

    System.out.println();
    parser.writeAnalysis();

    System.out.println("Time taken " + (sw.elapsed(TimeUnit.SECONDS)) + " seconds");
    sw.stop();
  }
}