package com.spark.assessment.scala.only.test

import com.spark.assessment.scala.only.Operations
import org.scalatest.FunSuite
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterAll


class BaselineAnalysisTest extends FunSuite with Matchers with BeforeAndAfterAll {
  
  
  test("TestCase 1: Validating the Number of records in a single DS") {
    
    /*
     * A sample test case is written here. 
     * Many other test cases can be implemented depending on scenarios.
     */

		val obj = new Operations
				val productList = List("barley","beef", "corn", "cotton", "pork","rice", "wheat")
				val barleyDS = obj.creatingDataStructures("barley")

				print("length = "+ barleyDS.length)
				
				assert(barleyDS.length == 14)				

	}
    
}