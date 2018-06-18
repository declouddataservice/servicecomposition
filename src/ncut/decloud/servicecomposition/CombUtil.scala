package ncut.decloud.service3

import java.util.Date

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object CombUtil {

  /**
    * print the bucketservicecompositon result on console
    * @param query user query formed as conjunctive query, an instance of Class QueryService
    * @param candidateComb candiate service compositions
    * @param execplans  final executable equal plans and contained plans
    * @param timestampList  timestamp of each phase
    */
  def printResult(query:QueryService, candidateComb: Set[Set[QueryService]], execplans:(Set[QueryService],Set[QueryService]),timestampList:List[Date]): Unit ={
    val executableEqPlans = execplans._1
    val executableInPlans = execplans._2

    val creBucketStart = timestampList.head
    val bucketEnd = timestampList(1)
    val genCandiComStart = timestampList(2)
    val genCandiComEnd = timestampList(3)
    val genCandiQStrat = timestampList(4)
    val genCandiQEnd = timestampList(5)

    var bucketsizes:Set[Int] = Set()
    query.subQueries.foreach(x => {println(x.name + "'s bucket size is :" + x.bucket.size); println(x.bucket);bucketsizes+=x.bucket.size})
    println()

    println("Equal plans are : ")
    executableEqPlans.foreach(x=> printEqQueries(x))
    println()
    println("Contained plans are : ")
    executableInPlans.foreach(x=> printInQueries(x))
    println()
    val totalPlans:Long = executableEqPlans.size + executableInPlans.size
    println("Candiate plansNum is : " + candidateComb.size)
    println("Execuatable equal plansNum is : " + executableEqPlans.size)
    println("Execuatable contained plansNum is : " + executableInPlans.size)
    println("Creating buckets costs : ")
    val t1 = printTimeCost(creBucketStart,bucketEnd)
    println("Finding all possible combinations costs : " )
    val t2 = printTimeCost(genCandiComStart,genCandiComEnd)
    println("Generating candidate queries costs : ")
    val t3 = printTimeCost(genCandiQStrat,genCandiQEnd)

    println("Total plans is : " + (executableEqPlans.size+executableInPlans.size))
    println("Total time is : " + (t1+t2+t3))

    if(totalPlans >0) {
      val tt = (t1 + t2 + t3).toFloat
      println("Per plan time is : ", tt / totalPlans)
    }

  }


  /**
    * generate the map of join attributes in query and their corresponding relation set, for example : "ID" -> {Movie, Revenue}
    * @param Q user query formed as conjunctive query, an instance of Class QueryService
    * @return the map formed as key is type of string denoting the join attribute, value is type of list which loads sub-goals owning this key attribute in query
    */
  def genQueryJoinAttrs(Q: QueryService) :Map[String, ListBuffer[QueryService]] = {
    val queryMap:mutable.Map[String, ListBuffer[QueryService]] = mutable.Map()
    Q.subQueries.foreach(g => {
      g.oColumns.foreach(attr => {
        queryMap.get(attr) match {
          case Some(n) => n.append(g)
          case None => queryMap+= (attr -> ListBuffer(g))
        }
      })
    })
    queryMap.filter(e => e._2.size > 1).toMap
  }

  /**
    * build a services and service instances graph, each service or instance denotes a graph node, and each edge between two nodes denotes the join attribute of them
    * this method is to check whether the service composition could join each other to generate a candidate rewriting
    * @param services a set of services and service instances, each of them is an instance of Class QueryService
    * @param joinAttrInQ join attributes and its corresponding map to sub-goals in query
    * @return
    */
  def initServiceGraph(services:Set[QueryService], joinAttrInQ:Map[String, ListBuffer[QueryService]]): ServiceGraph ={
    val g1 = new ServiceGraph()
    services.foreach({v =>
      if(v.oColumns == null){
        var unionClmns = Set[String]()
        v.subQueries.foreach(x => unionClmns = x.oColumns.union(unionClmns))
        v.oColumns = unionClmns
      }
      g1.addNode(v,joinAttrInQ)
    })
    g1
  }

  /**
    * check whether the two given data constraints have intersect,
    * @param cst1 data constraints formed as a tuple (attribute, left of the interval, right of the interval)
    * @param cst2 data constraints formed as a tuple (attribute, left of the interval, right of the interval)
    * @return if have intersect then return the intersect and true, else return false
    */
  def cstCompare(cst1:(String,Int,Int), cst2:(String,Int,Int)):(Int,Int,Boolean) = {
    if (!(cst1._2>cst2._3||cst1._3<cst2._2 || cst2._2>cst1._3 ||cst2._3<cst1._2)){
      val left = math.max(cst1._2,cst2._2)
      val right = math.min(cst1._3,cst2._3)

      (left,right,true) //intersect
    }
    else
      (0,0,false) //disjoint
  }



  /**
    * check the executable of candidate rewriting service compositions
    * @param candiateQueries a set of candidate service composition
    * @param query user query formed as conjunctive query, an instance of Class QueryService
    * @return executable equal rewriting plan collection and executable contianed rewriting plans collection
    */
  def findExecPlans(candiateQueries : Set[QueryService], query:QueryService):(Set[QueryService],Set[QueryService]) = {
    //    var plansNum = 0
    var executableEqPlans = Set[QueryService]() //a set of executable equal rewriting plan
    var executableInPlans = Set[QueryService]() //a set of executable contained rewriting plan
    val rangeMap = mutable.HashMap[Set[(String,Int,Int)],Boolean]()

    candiateQueries.foreach(q => {
      //q.oColumns is not empty indicates q is a candidate service composition generated from method genCandiateQuery
        //get additional constraints
        val q_query = q.dataContains(query) //check if q contains query, and if q intersects with query
        var A = CombUtil.genIntersect(Array(q.constraints,query.constraints)).diff(q.constraints)
        val T = CombUtil.genInterWindow(q.subQueries.toArray)

        q.subQueries.filter(x => x.serviceType == 0).foreach(service => {
          A = service.genInstanc(service.oColumns & query.oColumns, "", A, T)
        })

        if(A.isEmpty){
          //if empty then can generate the plan

            //check the relation of contain
            if(q_query._2.equals("contain")){
              executableEqPlans += q
            }else if(q_query._2.equals("overlap")){
              if(!rangeMap.getOrElse(q.constraints,false)){
                executableInPlans += q
                rangeMap.put(q.constraints,true)
              }
            }
        }
    })
    (executableEqPlans,executableInPlans)
  }

  /**
    * gen the projection v to q
    * @param v
    * @param q
    * @return
    */
  def getProjDC(v:QueryService, q:QueryService) : Set[(String, Int, Int)] = {
    genIntersect(Array(v.constraints, q.constraints)).filter(x => v.subQueries.flatMap(y=>y.oColumns ).contains(x._1) )
  }

  /**
    * get the intersect of constraints
    * @param arrCst constraints array
    * @return intersect of the constraints array
    */
  def genIntersect(arrCst : Array[Set[(String, Int, Int)]]) :Set[(String, Int, Int)] = {
    //the union of data constraints

    val unionCsts = arrCst.flatten
    //reduce data constraints, check whether there is a constraint conflict
    val combinCsts : Set[(String,Int,Int)] = Set()
    var hasIntersect = true
    val combinResult = unionCsts.groupBy(_._1).map{x =>
      x._2.reduce{(_1,_2) =>
        val r = cstCompare(_1,_2)
        if(!r._3)
          hasIntersect = false
        (x._1,r._1,r._2)
      }
    }.toSet
    if(hasIntersect)
      combinResult
    else
      null
  }

  /**
    * get the time intersect of services
    * @param arrService
    * @return
    */
  def genInterWindow(arrService : Array[QueryService]) : (Int,Int) = {
    var j = 0
    var range = 2147483647
    var slide = 0
    while(j < arrService.length) {
      if(arrService(j).windows != null){
        if(arrService(j).windows._1 < range){
          range = arrService(j).windows._1
        }
        if(arrService(j).windows._2 > slide){
          slide = arrService(j).windows._2
        }
      }
      j+=1
    }
    (range,slide)
  }

  /**
    * generate a candidate rewriting service composition based on the all possible service combination by Cartesian product,
    * mainly determines whether time-windows and constraints are mutually satisfied in the combination
    * @param vs a composition of services which is going to be determined whether is a candidate rewriting
    * @param query user query formed as conjunctive query, an instance of Class QueryService
    * @param g1 the service graph
    * @param algorithm  algorithm type, 0 denote the bucket algorithm
    * @return the checked composition q, if q.oColumns is empty then q is not a candidate composition, else is a candidate composition
    */
  def genCandiateQuery(vs : Set[QueryService],query : QueryService, g1: ServiceGraph,algorithm: Int,id:Int): QueryService ={
    val arrVs = vs.toArray
    //get the time-window intersect
    val T = genInterWindow(arrVs)
    var canJoin1 = true
    if(algorithm == 0){
      //check whether these services in vs can join
      arrVs.foreach(_.visited=false)
      val b = new ArrayBuffer[QueryService]()
      g1.dfs(arrVs(0),vs,b,arrVs(0).oColumns,query)
      if(vs.size == 1){
        canJoin1 = true
      }else{
        canJoin1 = b.toSet.size == vs.size
      }
    }
    if(canJoin1) {
      //the union of data constraints
      val A = genIntersect(arrVs.map(x => x.constraints))
      //if satisfied the constraints then return the q
      if(A !=null ){
        new QueryService("rw"+id,vs,A,T)
      }
      else
        null
    }else{
      null
    }
  }




  /**
    * print the executable equal service composition on console
    * @param q an equal executable plan
    */
  def printEqQueries(q: QueryService): Unit ={
    if(q.constraints.isEmpty){
      println("Equivalent query consists of services :")
      println(q.serviceType + " -- " + q.name + " -- " + q.subQueries + " -- " + q.oColumns + " -- " +  q.constraints + " -- " +q.windows)
      q.subQueries.foreach(x => println(x.serviceType + "--" + x.name + "--" + x.subQueries + "--" +x.oColumns + "--" +  x.constraints + " -- " +x.windows))
    }else{
      println("Equivalent query consists of services :")
      println(q.serviceType + " -- " + q.name + " -- " + q.subQueries + " -- " + q.oColumns + " -- " +  q.constraints + " -- " +q.windows)
      q.subQueries.foreach(x => {
        println(x.serviceType + "--" + x.name + "--" + x.subQueries + "--" +x.oColumns + "--" +  x.constraints + " -- " +x.windows)
        if(x.ops != null){
          println(", and ops is : " + x.ops)
        }
        println()
      })
    }
  }

  /**
    * print the executable contained service composition on console
    * @param q a contained executable plan
    */
  def printInQueries(q: QueryService): Unit ={
    if(q.constraints.isEmpty){
      println(q.serviceType + " -- " + q.name + " -- " + q.subQueries + " -- " + q.oColumns + " -- " +  q.constraints + " -- " +q.windows)
      println()
      println("Contained query consists of services :")
      q.subQueries.foreach(x =>println(x.serviceType + "--" + x.name + "--" + x.subQueries + "--" +x.oColumns + "--" +  x.constraints + " -- " +x.windows))
      println()
    }else{

      println(q.serviceType + " -- " + q.name + " -- " + q.subQueries + " -- " + q.oColumns + " -- " +  q.constraints + " -- " +q.windows)
      println()
      println("Contained query consists of services :")
      q.subQueries.foreach(x => {
        println(x.serviceType + "--" + x.name + "--" + x.subQueries + "--" +x.oColumns + "--" +  x.constraints + " -- " +x.windows)
        if(x.ops != null){
          println(", and ops is : " + x.ops  + "     ")
        }
      })
      println()
    }
  }

  /**
    * print the time cost based on the two given timestamps
    * @param starttime start timestamp
    * @param endtime end timestamp
    * @return the interval of the two timestamps
    */
  def printTimeCost(starttime:Date,endtime:Date): Long ={
    val between = endtime.getTime - starttime.getTime
    val day = between / (24 * 60 * 60 * 1000)
    val hour = between / (60 * 60 * 1000) - day * 24
    val min = (between / (60 * 1000)) - day * 24 * 60 - hour * 60
    val s = between / 1000 - day * 24 * 60 * 60 - hour * 60 * 60 - min * 60
    val ms = between - day * 24 * 60 * 60 * 1000 - hour * 60 * 60 * 1000 - min * 60 * 1000 - s * 1000
    println(min + "min " + s + "s " + ms + "ms ")
    (min*60+s)*1000 + ms
  }

//  /**
//    * 打印消耗的时间：基于一个时间段
//    * @return
//    */
//  def printTimeCost(between:Long): Long ={
//    //    val between = endtime.getTime - starttime.getTime
//    val day = between / (24 * 60 * 60 * 1000)
//    val hour = between / (60 * 60 * 1000) - day * 24
//    val min = (between / (60 * 1000)) - day * 24 * 60 - hour * 60
//    val s = between / 1000 - day * 24 * 60 * 60 - hour * 60 * 60 - min * 60
//    val ms = between - day * 24 * 60 * 60 * 1000 - hour * 60 * 60 * 1000 - min * 60 * 1000 - s * 1000
//    println("Time cost is : " + min + "分" + s + "秒" + ms + "毫秒")
//    s*1000 + ms
//  }

}
