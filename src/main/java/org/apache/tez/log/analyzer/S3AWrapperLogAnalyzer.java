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

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tries to parse logs generated via https://github.com/rajeshbalamohan/hadoop-aws-wrapper
 * This is useful for checking the access patterns in S3.
 * Get the yarn logs for the application and this would be able to generate the output
 * which can be fed to 3D charts.
 */
public class S3AWrapperLogAnalyzer extends BaseAnalyzer {
  // e.g 2016-10-26T08:49:30,369 INFO  [IO-Elevator-Thread-3 (1477292142883_0092_1_02_000005_0)]
  // org.apache.hadoop.fs.s3a.wrapper.S3AWrapperInputStream: hashCode_940752212,
  // s3a://bucket/tpcds_bin_partitioned_orc_200.db/store_sales/ss_sold_date_sk=2452247/000855_0,
  // readFully,25825534,0,0,12006847,1905375,99714748,
  private final Pattern s3aPattern =
      Pattern.compile(",(s3a.*),(.*),(\\d+),(\\d+),(\\d+),(\\d+),(\\d+),(\\d+),");

  private FileWriter writer;
  private File file = new File(".", "s3a.txt");

  @Override
  public void process(String line) throws IOException {
    if (line.contains("S3AWrapperInputStream")) {
      if (writer == null) {
        writer = new FileWriter(file);
        System.out.println("Starting writing to " + file.getAbsolutePath());

        writer.write(getHeader());
      }
      Matcher matcher = s3aPattern.matcher(line);
      while (matcher.find()) {
        String fileName = matcher.group(1);
        String op = matcher.group(2);
        String contentLen = matcher.group(3);
        String oldPos = matcher.group(4);
        String realPos = matcher.group(5);
        String positionalRead = matcher.group(6);
        String bytesRead = matcher.group(7);
        long timeInMillis = Long.parseLong(matcher.group(8)) / (1000 * 1000);

        StringBuilder sb = new StringBuilder();
        sb.append(fileName).append(",");
        sb.append(op).append(",");
        sb.append(contentLen).append(",");
        sb.append(oldPos).append(",");
        sb.append(realPos).append(",");
        sb.append(positionalRead).append(",");
        sb.append(bytesRead).append(",");
        sb.append(timeInMillis).append("\n");

        writer.write(sb.toString());
      }
    }
  }

  private String getHeader() {
    StringBuilder sb = new StringBuilder();
    sb.append("name").append(",");
    sb.append("op").append(",");
    sb.append("contentLen").append(",");
    sb.append("oldPos").append(",");
    sb.append("positionalRead").append(",");
    sb.append("bytesRead").append(",");
    sb.append("timeInMillis").append("\n");
    return sb.toString();
  }

  @Override
  public String getAnalysis() throws IOException {
    IOUtils.closeQuietly(writer);
    return null;
  }

  @Override
  public String getName() {
    return "S3AWrapperLogAnalyzer";
  }

  @Override
  public Object getResult() {
    IOUtils.closeQuietly(writer);
    System.out.println("Successfully wrote s3a.txt locally");
    return null;
  }
}
