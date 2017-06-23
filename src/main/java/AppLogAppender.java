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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * If app logs are already in HDFS, it is easier to parse via TFile loader.
 * But in some cases, we get text logs. So we need to append
 * "machineName" to get the container details etc. This util just prepends
 * machine name, container fileId to every line.
 *
 * e.g of container log would start with
 *
 * Container: container_e02_1468193113534_356593_01_006910 on pxnhd101.hadoop.local_45454
 */
public class AppLogAppender {

  static final Pattern CONTAINER = Pattern.compile("Container: (.*) on (.*)");

  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.out.println("Usage: java -cp ./target/blah.jar AppAppendLogger <file1_to_be read> "
          + "<file2_to_be written>");
    }
    try(BufferedReader reader = new BufferedReader(new FileReader(new File(args[0])))) {
      try(BufferedWriter writer = new BufferedWriter(new FileWriter(new File(args[1])))) {
        String machine = "";
        String container = "";
        while (reader.ready()) {
          String line = reader.readLine();
          if (line.contains("Container: ")) {
            Matcher matcher = CONTAINER.matcher(line);
            while (matcher.find()) {
              container = matcher.group(1);
              machine = matcher.group(2);
            }
          }
          if (!machine.isEmpty() && !container.isEmpty()) {
            writer.write(machine + "," + container + "," + line);
            writer.newLine();
          }
        }
      }
    }
    System.out.println("Done writing the file");
  }
}
