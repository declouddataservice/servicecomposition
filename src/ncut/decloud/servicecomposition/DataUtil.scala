package ncut.decloud.service3


object DataUtil {
  val POSI:Int = Double.PositiveInfinity.toInt  //Positive infinity
  val NEGA:Int = Double.NegativeInfinity.toInt  //Negative infinity

  //data source
  val vesselinfo = new QueryService("vesselinfo", Set("mmsi","imo","callsign", "name","type","length","width","positionType","eta","draught"))
  val vesseltraj = new QueryService("vesseltraj", Set("mmsi","long","lat","speed"))
  val vesseltravelinfo = new QueryService("vesseltravelinof", Set("imo","dest","source" ))

  val Movie = new QueryService("Movie",Set("ID", "Title", "Year", "Genre","Dir"))
  val Revenues = new QueryService("Revenues", Set("ID", "Amount"))
  val Director = new QueryService("Director", Set("Dir", "Age"))

  //service instances and services
  val S1 = new QueryService("S1",Set(vesseltraj,vesselinfo), Set("mmsi", "draught", "speed"),Set(("imo",NEGA,2000)),(5,2))
  val S2 = new QueryService("S2",Set(vesseltravelinfo), Set( "dest", "source"),Set(("imo",3000,POSI)),(5,2))
  val S3 = new QueryService("S3",Set(vesseltraj),Set("mmsi", "speed"),Set(("speed",NEGA,30)),(5,1))
  val S4 = new QueryService("S4",Set(vesseltravelinfo,vesselinfo), Set("mmsi", "draught", "dest"), Set(("imo",3000, POSI),("speed",30,POSI)),(5,2))
  val S5 = new QueryService("S5",Set(vesseltraj,vesselinfo), Set(("mmsi",1000, POSI)),(5,1))

  //queries
  val query1 = new QueryService("Q1", Set(Movie, Revenues,Director),Set("Title","Year","Dir"),Set(("Amount",100,POSI)),(5,2))
  val query2 = new QueryService("Q2", Set(vesseltravelinfo, vesseltraj,vesselinfo),Set("mmsi", "draught","speed","dest"),Set(("speed",40,POSI)),(5,4))
  val query3 = new QueryService("Q3", Set(vesselinfo, vesseltraj),Set("mmsi","callsign"),Set(("speed",40,POSI)),(5,4))
  //appoint the query
  val query:QueryService = query2
  // using simulated exposing service source
  val simulSource:Array[QueryService] = Array[QueryService](Movie,Revenues,Director,vesseltraj,vesseltravelinfo,vesselinfo)
  var simuServices:List[QueryService] = SourceSImulation.geneViews(DataUtil.simulSource,query,100)
  // using manually exposing service source
  val ucServices = List(S1,S2,S3,S4,S5)

}

