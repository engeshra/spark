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

package org.apache.spark.graphx.lib

import scala.reflect.ClassTag

import org.apache.spark.util.LongAccumulator
import org.apache.spark.graphx._

/** Connected components algorithm. */
object ConnectedComponents {
  /**
   * Compute the connected component membership of each vertex and return a graph with the vertex
   * value containing the lowest vertex id in the connected component containing that vertex.
   *
   * @tparam VD the vertex attribute type (discarded in the computation)
   * @tparam ED the edge attribute type (preserved in the computation)
   * @param graph the graph for which to compute the connected components
   * @param maxIterations the maximum number of iterations to run for
   * @return a graph with vertex attributes containing the smallest vertex in each
   *         connected component
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                      maxIterations: Int): Graph[VertexId, ED] = {
    require(maxIterations > 0, s"Maximum of iterations must be greater than 0," +
      s" but got ${maxIterations}")

    val ccGraph = graph.mapVertices { case (vid, _) => vid }
    def sendMessage(edge: EdgeTriplet[VertexId, ED]): Iterator[(VertexId, VertexId)] = {
      if (edge.srcAttr < edge.dstAttr) {
        Iterator((edge.dstId, edge.srcAttr))
      } else if (edge.srcAttr > edge.dstAttr) {
        Iterator((edge.srcId, edge.dstAttr))
      } else {
        Iterator.empty
      }
    }
    val initialMessage = Long.MaxValue
    val pregelGraph = Pregel(ccGraph, initialMessage,
      maxIterations, EdgeDirection.Either)(
      vprog = (id, attr, msg) => math.min(attr, msg),
      sendMsg = sendMessage,
      mergeMsg = (a, b) => math.min(a, b))
    ccGraph.unpersist()
    pregelGraph
  } // end of connectedComponents

  /**
   * Compute the connected component membership of each vertex and return a graph with the vertex
   * value containing the lowest vertex id in the connected component containing that vertex.
   *
   * @tparam VD the vertex attribute type (discarded in the computation)
   * @tparam ED the edge attribute type (preserved in the computation)
   * @param graph the graph for which to compute the connected components
   * @return a graph with vertex attributes containing the smallest vertex in each
   *         connected component
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[VertexId, ED] = {
    run(graph, Int.MaxValue)
  }

  def runWithAnalytics[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], maxIterations: Int, analytics: (LongAccumulator, LongAccumulator, LongAccumulator)): Graph[VertexId, ED] =
  {
    require(maxIterations > 0, s"Maximum of iterations must be greater than 0," +
      s" but got ${maxIterations}")
    // require(tol >= 0, s"Tolerance must be no less than 0, but got ${tol}")
    // require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belong" +
    //   s" to [0, 1], but got ${resetProb}")

    // val logger = new GraphXLogger("pageRank","pageRank","pageRank")
    // val personalized = srcId.isDefined
    // val src: VertexId = srcId.getOrElse(-1L)
    var (inOutMsgs, avgExc, numberOfReduce) = analytics

    val ccGraph = graph.mapVertices { case (vid, _) => vid }

    // def vertexProgram(id: VertexId, attr: (Long, Long), msg: Long): VertexId = {
    //   val startTime: Long = System.nanoTime()
    //   val min = math.min(attr, msg)
    //   avgExc.add(System.nanoTime() - startTime)
    //   min
    // }

    def sendMessage(edge: EdgeTriplet[VertexId, ED]): Iterator[(VertexId, VertexId)] = {
      inOutMsgs.add(1)
      if (edge.srcAttr < edge.dstAttr) {
        Iterator((edge.dstId, edge.srcAttr))
      } else if (edge.srcAttr > edge.dstAttr) {
        Iterator((edge.srcId, edge.dstAttr))
      } else {
        Iterator.empty
      }
    }

    def messageCombiner(a: Long, b: Long): Long =  {
      numberOfReduce.add(1)
      math.min(a, b)
    }

    val initialMessage = Long.MaxValue
    val pregelGraph = Pregel(ccGraph, initialMessage,
      maxIterations, EdgeDirection.Either)(
      vprog = (id, attr, msg) => {
        val startTime: Long = System.nanoTime()
        val min = math.min(attr, msg)
        avgExc.add(System.nanoTime() - startTime)
        min
      },
      sendMessage,
      messageCombiner)
    ccGraph.unpersist()
    pregelGraph
    // // var inOutMsgs: Long = 0
    // // var vertexProgramRuns: Long = 0
    // // var avgRunTime: Long = 0 
    // // Initialize the pagerankGraph with each edge attribute
    // // having weight 1/outDegree and each vertex with attribute 1.0.
    // val pagerankGraph: Graph[(Double, Double), Double] = graph
    //   // Associate the degree with each vertex
    //   .outerJoinVertices(graph.outDegrees) {
    //     (vid, vdata, deg) => deg.getOrElse(0)
    //   }
    //   // Set the weight on the edges based on the degree
    //   .mapTriplets( e => 1.0 / e.srcAttr)
    //   // Set the vertex attributes to (initialPR, delta = 0)
    //   .mapVertices { (id, attr) =>
    //     if (id == src) (1.0, Double.NegativeInfinity) else (0.0, 0.0)
    //   }
    //   .cache()

    // var pregelProgram = null 

    // // Define the three functions needed to implement PageRank in the GraphX
    // // version of Pregel


    // def personalizedVertexProgram(id: VertexId, attr: (Double, Double),
    //   msgSum: Double): (Double, Double) = {
    //   val (oldPR, lastDelta) = attr
    //   var teleport = oldPR
    //   val delta = if (src==id) resetProb else 0.0
    //   teleport = oldPR*delta

    //   val newPR = teleport + (1.0 - resetProb) * msgSum
    //   val newDelta = if (lastDelta == Double.NegativeInfinity) newPR else newPR - oldPR
    //   (newPR, newDelta)
    // }

    // def sendMessage(edge: EdgeTriplet[(Double, Double), Double]) = {
    //   // logger.logIncomingMsg("pageRankDataset","pageRankProgram", edge.dstId, 
    //   //   Pregel.getCurrentIteration, 1)
    //   // logger.logOutgoingMsg("pageRankDataset","pageRankProgram", edge.srcId, 
    //   //   Pregel.getCurrentIteration, 1)
    //   // inOutMsgs += 1
    //   inOutMsgs.add(1)
    //   if (edge.srcAttr._2 > tol) {
    //     Iterator((edge.dstId, edge.srcAttr._2 * edge.attr))
    //   } else {
    //     Iterator.empty
    //   }
    // }

    // The initial message received by all vertices in PageRank
    // val initialMessage = if (personalized) 0.0 else resetProb / (1.0 - resetProb)


    // // Execute a dynamic version of Pregel.
    // val vp = if (personalized) {
    //   (id: VertexId, attr: (Double, Double), msgSum: Double) =>
    //     personalizedVertexProgram(id, attr, msgSum)
    // } else {
    //   (id: VertexId, attr: (Double, Double), msgSum: Double) =>
    //     vertexProgram(id, attr, msgSum)
    // }

    // Pregel(pagerankGraph, initialMessage, activeDirection = EdgeDirection.Out)(
    //   vp, sendMessage, messageCombiner)
    //   .mapVertices((vid, attr) => attr._1)
  } // end of deltaPageRank
}
