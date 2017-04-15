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

package org.apache.spark.profiler

import org.apache.log4j._
// import org.slf4j.{Logger, LoggerFactory}
// import org.slf4j.impl.StaticLoggerBinder

import org.apache.spark.util.Utils

/**
 * Profiler of the graph application needs to log information about the applications
 * to be used in future prediction for any new application
 * Log4j is used for logging in a separated file, away from spark logger
 */
class GraphXLogger (val loggerName: String, val applicationName: String, 
	val logPath: String) extends Serializable {
	private var logObject: Logger = null
	private var application: String = applicationName
	private var logName: String = loggerName
	private var logFilePath: String= logPath
	private var initialized = false

	/**
	 * Constructor of the logger object
	 */
	protected def log: Logger = {
		if(logObject == null) {
			initialize();
			logObject = Logger.getLogger("profilerLog")
		}
		logObject
	}

	/**
	 * Intializer for the file appender using file configuration file
	 */
	private def initialize(): Unit = {
		println("+++++++++++ initialize logger +++++++++++++++++")
		if(!this.initialized) {			  
			// var fileAppender = new FileAppender
			// println("FilePath_log"+fileAppender.getFile())
			// LogManager.getLogger(this.logName)
		  // val log4j12Initialized = LogManager.getRootLogger.getAllAppenders.hasMoreElements
		  if(true) {
			  val fileAppendtLogProps = "org/apache/spark/log4j-file-appender.properties"
	      Option(Utils.getSparkClassLoader.getResource(fileAppendtLogProps)) match {
	        case Some(url) =>
	          PropertyConfigurator.configure(url)
	          System.err.println(s"Using Spark's custom log4j profile for profiling: $fileAppendtLogProps")
	        case None =>
	          System.err.println(s"Spark was unable to load $fileAppendtLogProps")
	      }

	      this.initialized = true
	    }
		}
	} 

	/**
	 * Logging the execution time for each vertex program 
	 *
	 * @param dataset_name the running dataset name
	 * @param program_name the runnning program name
	 * @param vertex_id the vertex that run its code now
	 * @param iteration_index the index of the current working superstep
	 * @param executionTime the time needed to commplete the program running in ns
	 */
	def logVPExecutionTime(dataset_name: String, program_name: String, 
		vertex_id: Double, iteration_index: Integer, executionTime: Long): Unit = {
		println("++++++++ Logging execution time +++++++++++++++++++")
    log.info("[ExecutionTime]"+"["+
    	dataset_name + "|"+
    	program_name + "|"+
    	vertex_id + "|"+
    	iteration_index+ "|"+
    	executionTime
    	+"]")
  }

	/**
	 * Logging any incoming message from any vertex
	 *
	 * @param dataset_name the running dataset name
	 * @param program_name the runnning program name
	 * @param vertex_id the vertex that run its code now
	 * @param iteration_index the index of the current working superstep
	 * @param message_number always equal 1
	 */
  def logIncomingMsg(dataset_name: String, program_name: String, 
		vertex_id: Double, iteration_index: Int, message_number: Long): Unit = {
  	println("++++++++ Logging incoming msg +++++++++++++++++++")
    log.info("[IncomingMsg]"+"["+
    	dataset_name + "|"+
    	program_name + "|"+
    	vertex_id + "|"+
    	iteration_index+ "|"+
    	message_number
    	+"]")
  }

	/**
	 * Logging any outgoing message from any vertex
	 *
	 * @param dataset_name the running dataset name
	 * @param program_name the runnning program name
	 * @param vertex_id the vertex that run its code now
	 * @param iteration_index the index of the current working superstep
	 * @param message_number always equal 1
	 */
  def logOutgoingMsg(dataset_name: String, program_name: String, 
		vertex_id: Double, iteration_index: Int, message_number: Long): Unit = {
  	println("++++++++ Logging outgoing msg +++++++++++++++++++")
    log.info("[OutgoingMsg]"+"["+
    	dataset_name + "|"+
    	program_name + "|"+
    	vertex_id + "|"+
    	iteration_index+ "|"+
    	message_number
    	+"]")
  }
}
