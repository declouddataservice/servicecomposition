package ncut.decloud.service3

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class ServiceGraph {
  var nodes:ListBuffer[QueryService] = _
//  var edge : Set[(ncut.decloud.servicecomposition.QueryService,String)] = _
  var path : ArrayBuffer[QueryService] = new ArrayBuffer[QueryService]()

  def addNode(service : QueryService, joinAttrInQ:Map[String, ListBuffer[QueryService]]): Unit ={
    if(service.adjNodes == null)
      service.genNode(new ListBuffer())

    if(nodes == null) {
      nodes = new ListBuffer()
      nodes.append(service)
    } else{
      val tempNodes = nodes.toList
      tempNodes.foreach({x =>
        val keys = x.oColumns & service.oColumns
        if(keys.nonEmpty && (keys & joinAttrInQ.keySet).nonEmpty ){
          val key = keys.head
          service.adjNodes.append((x,key))
          x.adjNodes.append((service,key))
        }
      })
      nodes.append(service)
    }
  }

  def dfs(service:QueryService,unvisited : Set[QueryService], path : ArrayBuffer[QueryService], attrs : Set[String], q : QueryService): Unit ={
    //service's adjacent is null or is empty, return false
    if(service.adjNodes!=null && service.adjNodes.nonEmpty) {
      service.visited = true
      path.append(service)
      val newUnvisited = unvisited - service
      var i = 0
      service.adjNodes.foreach { x =>
        //the adjacent node must hasn't been visited
        if (!x._1.visited) {
          x._1.visited = true
          if (newUnvisited.contains(x._1) && x._1.adjNodes.nonEmpty ){//&& !q.oColumns.subsetOf(attrs) ) {//&& !(x._1.subQueries== service.subQueries)
            if(i>0)
              path.append(service)
            dfs(x._1,newUnvisited, path,x._1.oColumns.union(service.oColumns), q)
            i += 1
          }
        }
      }
    }
  }
}
