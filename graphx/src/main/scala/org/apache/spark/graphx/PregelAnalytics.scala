package org.apache.spark.graphx

class PregelAnalytics {

  private var currentIteration: Int = 0
  private var numberOfShippedVertices: Long = 0

  def setNumberOfShippedVertices(shippedVertices: Long): Unit = {
    this.numberOfShippedVertices = shippedVertices
  }
  
  def setCurrentIteration(iteration: Int): Unit = {
    this.currentIteration = iteration
  }

  def getCurrentIteration(): Int = this.currentIteration

  def getNumberOfShippedVertices(): Long = this.numberOfShippedVertices
  
}