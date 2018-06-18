package ncut.decloud.service3

import scala.util.Random

object SourceSImulation {

  def geneViews(sourceRelations : Array[QueryService],query:QueryService,viewNum:Int): List[QueryService] = {
    val relatedNum = (viewNum*0.2).toInt
    val serviceNum = (relatedNum*0.2).toInt
    val serviceInstNum = relatedNum - serviceNum
    val unRelatedNum = viewNum - relatedNum
    val sourceArray = sourceRelations

    var relatedViews = Set[QueryService]() //views related to query
    var unRelatedViews =Set[QueryService]()//views not related to query
    var serviceInstances = 0
    var services = 0
    var i = 0

    while(relatedViews.size<relatedNum || unRelatedViews.size < unRelatedNum){
      i+=1
      var relationSet = Set[QueryService]() //sub-goals in view
      var oColumns = Set[String]()  //head attributes of view
      var constraints = Set[(String,Int,Int)]() //data constraints of view

      //add a random number of relations
      var relationNum = new Random().nextInt(sourceArray.length)
      if (relationNum<4)
        relationNum = 3
      while(relationNum>0){
        val currIndex = new Random().nextInt(sourceArray.length) //current index of sub-goal
        var joinNum = 0
          if (relationSet.exists(x => {
            (x.oColumns & sourceArray(currIndex).oColumns).nonEmpty
          }))
            joinNum += 1
        if(joinNum>0||relationSet.isEmpty)
          relationSet += sourceArray(currIndex)
        relationNum -= 1
      }

      //add a random number of output attributes
      var tempColumn = Set[String]()
      relationSet.foreach(x => tempColumn ++= x.oColumns) //output attributes collection
      val columnsArray = tempColumn.toArray
      var columnNum = new Random().nextInt(columnsArray.length)

      while (columnNum>0){
        val columnIndex = new Random().nextInt(columnsArray.length)
        oColumns += columnsArray(columnIndex)
        columnNum -= 1
      }

      //add a random number of data constraints
      var cstNum = new Random().nextInt(tempColumn.size)  //the number of constraints cannot exceed the number of output attributes
      while (cstNum>0){
        val columnIndex = new Random().nextInt(cstNum)
        val left = new Random().nextInt()
        val right = new Random().nextInt()
        if(!constraints.exists(x=>x._1.equals(columnsArray(columnIndex))))
          constraints += Tuple3(columnsArray(columnIndex),left,right)

        cstNum -= 1
      }

      //a random time-window between (1,1) to (10,10)
      var range = new Random().nextInt(10)
      if (range<1) range = 5
      var slide = new Random().nextInt(10)
      if (slide<1) slide = 2

      //service type is random
      val serviceType = new Random().nextInt(2)
      var tempView : QueryService = null
      if(oColumns.nonEmpty && serviceType==1){
        tempView = new QueryService("v" + i ,relationSet,oColumns,constraints,(range,slide))
      }else{
        tempView = new QueryService("v" + i ,relationSet,constraints,(range,slide))
      }

      var times = 0
      if(tempView.oColumns == null){
        var unionClmns = Set[String]()
        tempView.subQueries.foreach(x => unionClmns = x.oColumns.union(unionClmns))
        tempView.oColumns = unionClmns
      }

      //check if the view is related to the query
      if(tempView.winContains(query)){
        query.subQueries.foreach { g =>
          if (tempView.subQueries.contains(g)) {
            if ((g.oColumns & query.oColumns).subsetOf(tempView.oColumns & g.oColumns)) {
              var falseNum = 0
              query.constraints.foreach { cst1 =>
                tempView.constraints.foreach { cst2 =>
                  if (cst1._1.equals(cst2._1) && !CombUtil.cstCompare(cst1, cst2)._3 )
                    falseNum += 1
                }
              }
              if (falseNum == 0){
                times += 1
              }
            }
          }
        }
      }

      if (times>0){
        if(relatedViews.size<relatedNum){

          if(tempView.serviceType == 0 && services < serviceNum){
            services += 1
            relatedViews += tempView
          }

          else if(tempView.serviceType == 1 && serviceInstances < serviceInstNum){
            serviceInstances += 1
            relatedViews += tempView
          }
        }
      }else{
        if(unRelatedViews.size<unRelatedNum)
          unRelatedViews += tempView
      }
    }
    val viewSet = relatedViews.union(unRelatedViews)

    println("related size is : " , relatedViews.size," unrelated is : " , unRelatedViews.size)
    println("Service num is : " + services + " ; ServiceInstances num is : " + serviceInstances)
    viewSet.toList
  }


}
