/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.graphx.util

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel

import java.io._

/**
 * Provides utilities for loading [[Graph]]s from files.
 */
class GraphXStatisticsParser extends Logging {

  /**
   * Loads a application statistics from logging file where each line contains 
   * result of application run
   * 
   *
   *
   * @example Loads a file in the following format:
   * {{{
   * # Comment Line
   * # [statistic_type][application_name|dataset_name|vertex_id|iteration_index|value(time, #of_messages)]
   * }}}
   *
   * @param sc SparkContext
   * @param path the path to the logging file (e.g., /home/data/file or hdfs://file)
   */
  def statisticsResult(
      sc: SparkContext,
      logPath: String,
      resultPath: String,
      programName: String,
      datasetName: String,
      graph: Graph[Int, Int]): Unit =
  {
    val startTime = System.currentTimeMillis
    // Parse the edge data table directly into edge partitions
    val lines = sc.textFile(logPath)
    // Total sum of all factors
    val totalExecutionTime = sc.longAccumulator
    val totalInOutMessage = sc.longAccumulator
    // the statistics for each iteration, tuple _1 -> execution_time, _2 -> In/out Message
    var finalStatistics:Map[Int,(Long, Long)] = Map()
    val statisticsTypePattern = """\[([A-Za-z]|\d|\.)*\]""".r
    val statisticsPattern = """\[(([A-Za-z]|\d|\.)+\|)+([A-Za-z]|\d|\.)+\]""".r

    lines.foreach { line =>
      if (!line.isEmpty && line(0) != '#') {
        val statisticsType = (statisticsTypePattern findAllIn line).mkString(",")
        val statisticsStr = (statisticsPattern findAllIn line).mkString(",")
        val statisticsArray: Seq[String] = statisticsStr.substring(1, statisticsStr.length-1).split('|')
        if (statisticsType.isEmpty() || statisticsArray.length != 5) {
          throw new IllegalArgumentException("Invalid line: " + line)
        }

        val dataset_name = statisticsArray(0)
        val application_name = statisticsArray(1)
        val vertex_id = statisticsArray(2).toDouble
        val iteration_index = statisticsArray(3).toInt
        val value = statisticsArray(4)

        if(!finalStatistics.contains(iteration_index)) {
          finalStatistics += (iteration_index -> (0, 0))
        }
        if (statisticsType.equalsIgnoreCase("[ExecutionTime]")) {
          println("ExecutionTime: "+iteration_index)
          finalStatistics += (iteration_index -> 
              (value.toLong + finalStatistics(iteration_index)._1, finalStatistics(iteration_index)._2))
          totalExecutionTime.add(value.toLong)
        } else if (statisticsType.equalsIgnoreCase("[IncomingMsg]")) {
          println("IncomingMsg: "+iteration_index)
          finalStatistics += (iteration_index -> 
              (finalStatistics(iteration_index)._1, value.toLong + finalStatistics(iteration_index)._2))
          totalInOutMessage.add(value.toLong)
        } else if (statisticsType.equalsIgnoreCase("[OutgoingMsg]")) {
          println("OutgoingMsg: "+iteration_index)
          // finalStatistics += (iteration_in/dex -> 
          //     (finalStatistics(iteration_index)._1, value.toLong + finalStatistics(iteration_index)._2))
          // totalInOutMessage.add(value.toLong)
        } else {
          throw new IllegalArgumentException("Not defined type : " + statisticsType)
        }
      }
    }

    println("############### statistics ################")
    println(totalExecutionTime.value)
    println(totalInOutMessage.value)
    val avgExecution = totalExecutionTime.value/graph.vertices.count
    val totalInMessage = totalInOutMessage.value
    
    // finalStatistics.keys.foreach{ i =>  
    //    println( "iteration = " + i)
    //    println(" AvgExecutionTime = " + finalStatistics(i)._1/graph.vertices.count )
    //    println(" AvgInOutMessage = " + finalStatistics(i)._2/graph.vertices.count )
    //    println("###################################################################")
    // }
    val outputResult: String = datasetName+","+programName+","+avgExecution+","+totalInMessage;
    appendResultToFile(resultPath, outputResult)

    logInfo("It took %d ms to load the edges".format(System.currentTimeMillis - startTime))
  } 

  protected def appendResultToFile(
    filePath: String,
    line: String): Unit = 
  {
    val file = new File(filePath)
    // println(filePath)
    println(file.exists)
    var writer = if(!file.exists) (new PrintWriter(file)) else (new FileWriter(filePath,true))
    writer.write(line+"\n")
    writer.close()
  }

}
