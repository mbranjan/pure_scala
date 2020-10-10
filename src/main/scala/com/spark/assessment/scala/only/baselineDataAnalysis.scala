package com.spark.assessment.scala.only

import org.apache.log4j.{Level,Logger}
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConverters._

object baselineDataAnalysis {

	val log = Logger.getLogger(this.getClass.getName)

			def main(args: Array[String]):Unit ={   


					val configFilePath = "src/main/resources/config_file.conf"


							log.warn("************* Creating Operations Class Object *************")
							val opsObj = new Operations()

							log.warn("******** Calling function readinConfigurationFile **********")							
							val productList = opsObj.readingConfigurationFile(configFilePath).asScala.toList


							log.warn("************ Creating separate Datastructures **************")
							val listOfDS = productList.map(x=> opsObj.creatingDataStructures(x))


							log.warn("*******Joining the Datastructures for final result**********")
							val finalDS = opsObj.joiningDataStructures(listOfDS)


							log.warn("\n\n##########################################    DISPLAYING FINAL RESULT   #########################################\n")
							finalDS.foreach(println)

	}
}



case class barley(
		crop_year:String,
		production: String,
		country: String
		)
