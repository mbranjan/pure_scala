package com.spark.assessment.scala.only

import com.typesafe.config.ConfigFactory
import java.io.File
import scala.io.Source

class Operations {

	def readingConfigurationFile(configFilePath:String) = {

			/** 
			 *  In this method, we are reading values from property file  
			 *  
			 */  


			val confReader = ConfigFactory.parseFile(new File(configFilePath))

					val productList = confReader.getStringList("productList")

					(productList)
	}


	def creatingDataStructures(file_name:String): List[(String, String, String)] ={


	  /** 
			 *  In this method, we are creating different data structures from 
			 *  different source files using scala APIs like map, filter, groupBy etc   
			 *  
			 */
	  
	  
			val file_path= "src/test/resources/" + file_name
					val col2 = "world_" + file_name + "_harvest" 
					val col3 = "usa_" + file_name + "_contribution%"

					val lines = Source.fromFile(file_path).getLines.toList
					val data = lines.map(line=> barley(line.split("\t")(0),line.split("\t")(1),line.split("\t")(2)))
					val filteredData =data.filter(x=> x.production != "     --").filter(x => x.country != "Country")


					val dataByYear = filteredData.groupBy(x=> x.crop_year)


					val intermidiateData = dataByYear.map(x => x._2).map(x => (x.map(y=> y.crop_year).toSet.toList(0).toString(),x.filter(y => y.country == "USA").map(x=>x.production).toList(0).toFloat , x.map(y=> y.production.toLong).reduce(_ + _))).map(x=> (x._1.trim(),x._3.toString(), "%.2f".format((x._2 * 100 / x._3).toFloat).toString())).toList.sortBy(x => x._1)


					val finalDataWithHeader =  ("Year",col2,col3) +: intermidiateData.toList 


					return finalDataWithHeader

	}


	def joiningDataStructures(listOfDS:List[List[(String, String, String)]]): List[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)] = {

	  /** 
			 *  In this method, we are joining different data structures  
			 *  into one final data structure using Map collection.  
			 *  
			 */
	  
	  
			val barleyDS = listOfDS(0)
					val beefDS = listOfDS(1)
					val cornDS = listOfDS(2)
					val cottonDS = listOfDS(3)
					val porkDS = listOfDS(4)
					val riceDS = listOfDS(5)
					val wheatDS = listOfDS(6)

					val beefDSMap = beefDS.map(rec => (rec._1 -> (rec._2,rec._3))).toMap
					val cornDSMap = cornDS.map(rec => (rec._1 -> (rec._2,rec._3))).toMap
					val cottonDSMap = cottonDS.map(rec => (rec._1 -> (rec._2,rec._3))).toMap
					val porkDSMap = porkDS.map(rec => (rec._1 -> (rec._2,rec._3))).toMap
					val riceDSMap = riceDS.map(rec => (rec._1 -> (rec._2,rec._3))).toMap
					val wheatDSMap = wheatDS.map(rec => (rec._1 -> (rec._2,rec._3))).toMap

					val finalDS = barleyDS.flatMap{case (k1,v1,v2) => beefDSMap.get(k1).map(tup => (k1,v1,v2,tup._1,tup._2))}
			.flatMap{case (k1,v1,v2,v3,v4) => cornDSMap.get(k1).map(tup => (k1,v1,v2,v3,v4,tup._1,tup._2))}
			.flatMap{case (k1,v1,v2,v3,v4,v5,v6) => cottonDSMap.get(k1).map(tup => (k1,v1,v2,v3,v4,v5,v6,tup._1,tup._2))}
			.flatMap{case (k1,v1,v2,v3,v4,v5,v6,v7,v8) => porkDSMap.get(k1).map(tup => (k1,v1,v2,v3,v4,v5,v6,v7,v8,tup._1,tup._2))}
			.flatMap{case (k1,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10) => riceDSMap.get(k1).map(tup => (k1,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,tup._1,tup._2))}
			.flatMap{case (k1,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,v11,v12) => wheatDSMap.get(k1).map(tup => (k1,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,v11,v12,tup._1,tup._2))}


			//				val s = Seq(barleyDS,beefDSMap,cornDSMap,cottonDSMap,porkDSMap,riceDSMap,wheatDSMap)				
			//				s.reduce((x,y) => x.flatMap{case (k1,v1,v2) => y.get(k1).map(tup => (k1,v1,v2,tup._1,tup._2))} )

			return finalDS

	}


}