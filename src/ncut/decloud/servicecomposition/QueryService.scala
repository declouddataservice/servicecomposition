package ncut.decloud.service3

import scala.collection.mutable.ListBuffer

@SerialVersionUID(1L)
class QueryService extends Serializable {
  final val POSI = Double.PositiveInfinity.toInt  //Positive infinity
  final val NEGA = Double.NegativeInfinity.toInt  //Negative infinity

  var name: String = _  //service name
  var subQueries :Set[QueryService]= _  //sub-goals
  var oColumns: Set[String] = _ //output attributes
  var constraints: Set[(String,Int,Int)] = _  //data constraints
  var windows: (Int,Int) = _  //time-window
  var bucket: Set[QueryService] = Set[QueryService]() //bucket
  var ops : (Set[String],String,Set[(String,Int,Int)],(Int,Int)) = _
  var serviceType: Int= _

  var adjNodes: ListBuffer[(QueryService,String)] = _
  var visited:Boolean = false

  /**
    * as a node of service graph and initialize the adjacent nodes buffer
    * @param adjNodes the connected services and the primary keys
    */
  def genNode(adjNodes : ListBuffer[(QueryService,String)]){
    this.adjNodes = adjNodes
  }

  //Data Stream <name,attrs>
  def this(name: String,oColumns: Set[String]){
    this()
    this.name = name
    this.oColumns = oColumns
  }

  //Service Instance  <name,subqueries,attrs,dataconstraints,timeconstraints>
  def this(name: String,subQueries: Set[QueryService],oColumns: Set[String],constraints: Set[(String,Int,Int)],windows:(Int,Int)){
    this()
    this.name = name
    this.oColumns = oColumns
    this.constraints = constraints
    this.subQueries = subQueries
    this.windows = windows
    this.serviceType = 1
  }

  //Service <name,subqueries,dataconstraints,timeconstraints>
  def this(name: String,subQueries: Set[QueryService],constraints: Set[(String,Int,Int)],windows:(Int,Int)){
    this()
    this.name = name
    this.subQueries = subQueries
    this.constraints = constraints
    this.windows = windows
    this.serviceType = 0
  }

  /*
  genInstance
   */
  def genInstanc(input:Set[String], output: String, dataCsts : Set[(String,Int,Int)], timeCsts:(Int,Int)): Set[(String,Int,Int)] ={
    val satisfied = dataCsts.filter{x => this.oColumns.contains(x._1)}
    val unsatisfied = dataCsts &~ satisfied
    this.ops = (input,output,satisfied,timeCsts)
    //return constraints which are not in the service output attributes
    unsatisfied
  }

  /*
  getInstance
   */
  def getInstanc(input:Set[(String,Int,Int)], output: Set[String], timeCsts:(Int,Int)): QueryService ={
    val a = new QueryService(this.name + "_Inst", this.subQueries, this.oColumns, this.constraints, this.windows)
    a.genInstanc(output,"",input,timeCsts)
    a
  }

  /*
  add element to the service bucket
   */
  def addToBuckets(query: QueryService): Unit ={
    bucket += query
  }

  /*
  check the time-window contained
   */
  def winContains(otherQS : QueryService): Boolean ={
    val win1 = this.windows
    val win2 = otherQS.windows
    if(win1 == null || (win1._1 >= win2._1 && win1._2<=win2._2 && (win2._2%win1._2 == 0))){
      true
    } else false
  }

  /*
  check whether service a date contains service b, return the constraints intersect whose attributes are in a, while such constraints whose attributes are in b but not in a will not return
  */
  def dataContains(otherQS:QueryService) :(Set[(String,Int,Int)], String)={
    val csts1 = this.constraints
    val csts2 = otherQS.constraints

    var trueNum = 0
    var addCsts = Set[(String,Int,Int)]()

    csts1.foreach{cst1 =>
      if(!csts2.exists(x => x._1.equals(cst1._1))){//no such constraints with this attribute
      val addCompare = CombUtil.cstCompare(cst1,(cst1._1,NEGA,POSI))
        addCsts += Tuple3(cst1._1,addCompare._1,addCompare._2)
      }else{
        //if exist then check whether the corresponding constraint is satisfied
        csts2.foreach{cst2 =>
          if(cst2._1.equals(cst1._1)) {//find the constraint with the same attribute
          val comResult = CombUtil.cstCompare(cst1,cst2)
            if ((cst1._1, comResult._1, comResult._2).equals(cst2)){
              trueNum += 1
              addCsts += Tuple3(cst1._1,comResult._1,comResult._2)
            } //cst1 contains cst2
            else if(!comResult._3){
              trueNum = -1
            }//no intersect
            else{
              addCsts += Tuple3(cst1._1,comResult._1,comResult._2)
            }  //cst1 doesn't contain cst2
          }
        }
      }
    }
    if(trueNum == csts1.size){
      (addCsts,"contain") //contain
    }
    else if(trueNum == -1){
      (addCsts,"uncontain") //disjoint
    }
    else {
      (addCsts,"overlap")  //intersect
    }
  }


  /**
    * check whether service a contains service b based on the time-window check and data constraints check
    * @param otherQS service b
    * @return the contain relation between a and b , and the data constraints intersect
    */
  def contains(otherQS:QueryService):(Set[(String,Int,Int)],String)={
    val dataCst = dataContains(otherQS)
    if(!this.winContains(otherQS)){ //although time-window is not contained, the data constraint addCsts is still needed to find out
      (dataCst._1,"uncontain")
    }else{
      dataCst
    }
  }

  override def toString: String = {
    super.toString
    name
  }

  override def hashCode(): Int = {
    this.name.hashCode
  }

  override def equals(obj: scala.Any): Boolean = {
    if(this.hashCode()==obj.hashCode())true
    else false
  }
}


