package ncut.decloud.service3

import java.util.Date

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object SMinicon {

  //default constructor of MCD
  class MCD(Service: QueryService, subGoals: Set[QueryService]){
    var s : QueryService = Service
    var gc : Set[QueryService] = subGoals
    var projDC : Set[(String, Int, Int)] = Set()
    var mt : Int = 0 //init mt as type equal rewriting

    override def toString: String = {
      super.toString
      s.name + " " + gc.toString()
    }
  }

  var joinAttrInQ :Map[String, ListBuffer[QueryService]] = Map()
  var g1:ServiceGraph = _

  /**
    * expand the covered gc , and return the expanded MCD
    * @param querySubgoal current subgaal which is going to be expanded
    * @param service
    * @param query  the user query
    * @param joinAttrs  the join attributes map of query
    * @return the expanded MCD, and a tag denoting whether can expand
    */
  def genEachMCD(querySubgoal: QueryService, service: QueryService, query: QueryService, joinAttrs:Set[String]):(Boolean,MCD) = {
    val mcd = new MCD(service, Set())
    var canGenerate = false

    if (service.subQueries.contains(querySubgoal)){

      val serviceSubgoal = service.subQueries.find(_ == querySubgoal).get

      val querySubHead = query.oColumns & querySubgoal.oColumns
      val serviceSubHead = service.oColumns & serviceSubgoal.oColumns

      if (querySubHead.subsetOf(serviceSubHead)){
        mcd.gc += querySubgoal
        canGenerate = true
        val existentialAttrs = serviceSubgoal.oColumns &~ serviceSubHead  //existentialAttrs
        val joinAttrInService = existentialAttrs & joinAttrs  //extra join attr in service

        joinAttrInService.foreach(attr => {
          val subgoalsInAttr = joinAttrInQ.getOrElse(attr,Set())
          subgoalsInAttr.foreach({subgoal =>
            val returnValue = genEachMCD(subgoal, service, query, joinAttrs-attr)
            mcd.gc ++= returnValue._2.gc
            canGenerate &&= returnValue._1
          })
        })
      }
    }
    (canGenerate, mcd)
  }

  /**
    * generate all possible MCD for existing services/service instances
    * @param Q  a user query
    * @param S  set of services or service instances
    * @return set of MCD MCDs
    */
  def formMCDs(Q:QueryService, S: Set[QueryService]) :Set[MCD] = {
    //init MCDs
    var MCDs:Set[MCD] = Set()
    println("Start ......")

    val joinAttr = joinAttrInQ.keySet
    //querySubgoal: subgoal of Q; service: service in S; serviceSubgoal: subgoal in servcie.
    Q.subQueries.foreach(querySubgoal => {
      S.foreach(service => {
        if (service.winContains(Q)){
          val cmpQueryService = Q.dataContains(service)
          if (!MCDs.exists(x => x.s == service && x.gc.contains(querySubgoal)) && !cmpQueryService._2.equals("uncontain")){
            val result = genEachMCD(querySubgoal, service, Q, joinAttr)
            if (result._1){
              val mcd = result._2
              val cmpS_Q = service.dataContains(Q)
              if(cmpS_Q._2.equals("contain")) mcd.mt = 0
              else if(cmpS_Q._2.equals("overlap")) mcd.mt = 1
              mcd.projDC = CombUtil.getProjDC(service, Q)
              MCDs += mcd
            }
          }
        }
      })
    })
    MCDs
  }

  /**
    * combine the MCD of MCDs, generate a map which key is covered subgoals , value is the combinations of MCD who can covered the subgoals
    * @param MCDs a set of MCD
    * @return the MCD composition map
    */
  def combineMCDs(MCDs : Set[MCD]) :mutable.Map[Set[QueryService], Set[Set[MCD]]] = {
    val comMap :mutable.Map[Set[QueryService], Set[Set[MCD]]] = mutable.Map()
    MCDs.foreach(mcd => {
      comMap.keySet.foreach(key => {
        if ((mcd.gc & key).isEmpty ){
          comMap.get(key.union(mcd.gc)) match {
            case Some(coms) =>  comMap(key.union(mcd.gc)) = coms ++ comMap(key).map(x=> x+mcd)
            case _ =>  comMap.put(key.union(mcd.gc), comMap.getOrElse(key,Set()).map(com => com+mcd))
          }
        }
      })
      comMap.get(mcd.gc) match {
        case Some(coms) => comMap(mcd.gc) += Set(mcd)
        case _ => comMap.put(mcd.gc, Set(Set(mcd)))
      }
    })
    comMap
  }

  /**
    * based on the comMap, find the combinations of MCD who can covered all the subgoals of query , and generate the executable plans
    * @param comMap the map of MCD combinations
    * @param query  the user query
    * @return executable equal plans and contained plasn
    */
  def genRewritingService( comMap : mutable.Map[Set[QueryService], Set[Set[MCD]]], query: QueryService): (Set[QueryService], Set[QueryService])= {
    var executableEqPlans = Set[QueryService]() //a set of executable equal rewriting plan
    var executableInPlans = Set[QueryService]()
    var id = 0
    val rangeMap = mutable.HashMap[Set[(String, Int, Int)], Boolean]()

    comMap.getOrElse(query.subQueries, Set()).foreach { mcds =>
      var P: QueryService = null
      val A1 = CombUtil.genIntersect(mcds.map(mcd => mcd.projDC).toArray) //gen the intersect of all projDC

      if (A1 != null) {
        var A = A1.diff(mcds.flatMap(mcd => mcd.s.constraints)) // additional constraints
        val T = CombUtil.genInterWindow(mcds.map(mcd => mcd.s).toArray)
        mcds.map(mcd => mcd.s).filter(x => x.serviceType == 0).foreach(s => {
          A = s.genInstanc(s.oColumns & query.oColumns, "", A, T)
        })

        if (A.isEmpty) {
          P = new QueryService("rw"+id, mcds.map(mcd => mcd.s), CombUtil.genIntersect(mcds.map(mcd => mcd.s.constraints).toArray), T)
          id+=1

          if (!mcds.exists(mcd => mcd.mt == 1)) executableEqPlans += P
          else {
            if (!rangeMap.getOrElse(P.constraints, false)) {
              executableInPlans += P
              rangeMap.put(P.constraints, true)
            }
          }
        }
      }
    }
    (executableEqPlans, executableInPlans)
  }


  def main(args: Array[String]): Unit = {

    val timestamps = mutable.ListBuffer[Date]()//store the timestamps of all stages

    //data source
    val query = DataUtil.query
    val services = DataUtil.ucServices//data source in use case
//    val services = DataUtil.simuServices//simulated services and service instances

    //get the join attributes map , which key is the join attribute , value is the set of services who owns the join attribute
    joinAttrInQ = CombUtil.genQueryJoinAttrs(query)
    println(joinAttrInQ)

    //init the service graph and the attributes of services
    g1 = CombUtil.initServiceGraph(services.toSet,joinAttrInQ)

    //form MCD
    timestamps.append(new Date) //start time of form  mcds
    val MCDs = formMCDs(query,services.toSet)
    timestamps.append(new Date)


    //combine MCD
    timestamps.append(new Date)
    val comMap = combineMCDs(MCDs)
    timestamps.append(new Date)


    //get the rewriting services
    timestamps.append(new Date)
    val rewritings = genRewritingService(comMap,query)
    timestamps.append(new Date)

    //information of MCD
    MCDs.foreach(x => println(x.s.serviceType + "--"+ x.s + "--" + x.gc))

    //get the MCD compositons who can covered all the subgoals of query
    val s = comMap(query.subQueries).map(mcds => mcds.map(mcd => mcd.s))
    //print the result
    CombUtil.printResult(query,s,rewritings,timestamps.toList)

  }

}
