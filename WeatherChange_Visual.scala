import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import breeze.linalg.{DenseVector}
import breeze.plot.{plot, _}
object WeatherChange_Visual {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Former-USSR_Climate_Change")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    //一个观测点数据导入
    //    val pre_data1 = sc.textFile("C:\\Users\\yangjuncai\\Desktop\\大学学习\\大三下\\" +
    //      "大数据技术基础\\ndp040\\ndp040\\f29263.dat")
    val pre_data1 = sc.textFile("C:\\Users\\yangjuncai\\Desktop\\大学学习\\大三小学期\\大数据基础实训\\数据文件")
    //数据格式处理，将三个空格和两个空格都置换成一个空格，并切分数据。
    val fields = pre_data1.map(_.trim.replace("   ", " ")
      .replace("  ", " ").split(" "))
    //保留15个数据域的数据，并且符合QR为有观测数据和温度值不为999.9。
    //6\8\10位标志位不能为999，对数据用filter进行过滤
    val pre_ann1 = fields.filter(_.length == 15).filter(_(2)=="8")
      .filter(_(5)!="999").filter(_(6)!="9")
    /**
     * 第二问：同一时间窗口内各个观测点的气温的变化趋势
     */
    val pre_ann_map1: RDD[(((String, Int), String), Double)] = pre_ann1.map(f=>(((f(1),f(2).toInt),f(0)),f(5).toDouble))
    val Mean_Temp: RDD[(((String, Int), String), Iterable[Double])] = pre_ann_map1.groupByKey().sortByKey()
    val pre_ann_mean2: RDD[(((String, Int), String), Double)] = Mean_Temp
      .map(x=>(x._1,x._2.reduce(_+_) / x._2.size))
    val un: RDD[(String, Iterable[(String, Double)])] = pre_ann_mean2.map(x=>(x._1._2,(x._1._1._1,x._2))).groupByKey()
    val f=Figure()
    val p = f.subplot(0)
    p.legend=(true)

    /**
     * 最高纬度站点 （OSTROV DIKSON） ID:20674  北纬73.50  东经80.40
     */
    val separation_1 = un.filter(_._1.toInt==20674)
    val list1: List[Iterable[(String, Double)]] = separation_1.map(x => x._2).collect().toList
    val Highest_Latitude = list1.foreach(x=>{
      val array_high: Array[(String, Double)] = x.toArray
      val year1:Array[Float] = array_high.map(x=>(x._1.toFloat))
      val avgtmp1:Array[Float] = array_high.map(x=>(x._2.toFloat))
      //plot函数前后必须类型一致
      p += plot( DenseVector(year1), DenseVector(avgtmp1),style='-',colorcode="r",name="OSTROV DIKSON站点(北纬73.5°)")
      p += plot( DenseVector(year1), DenseVector(avgtmp1),style='+',colorcode="r",name="高纬")
      p.xlabel = "年份"
      p.ylabel = "8月份平均温度"
      p.title = ("不同纬度温度变化趋势图")
    })
    /**
     * 中间纬度站点(KURGAN) ID:28661  北纬55.47  东经65.40（纬度靠近莫斯科）
     */
    val separation_2 = un.filter(_._1.toInt==28661)
    val list2: List[Iterable[(String, Double)]] = separation_2.map(x => x._2).collect().toList
    val Midest_Latitude = list2.foreach(x=>{
      val array_mid: Array[(String, Double)] = x.toArray
      val year2:Array[Float] = array_mid.map(x=>(x._1.toFloat))
      val avgtmp2:Array[Float] = array_mid.map(x=>(x._2.toFloat))
      p += plot( DenseVector(year2), DenseVector(avgtmp2),style='-',colorcode="g",name="KURGAN站点(北纬55.47°)")
      p += plot( DenseVector(year2), DenseVector(avgtmp2),style='+',colorcode="g",name="中纬")
    })
    /**
     * 最低纬度站点(KUSKA) ID:38987  北纬35.28  东经62.35
     */
    val separation_3 = un.filter(_._1.toInt==38987)
    val list3: List[Iterable[(String, Double)]] = separation_3.map(x => x._2).collect().toList
    val Lowest_Latitude = list3.foreach(x=>{
      val array_mid: Array[(String, Double)] = x.toArray
      val year3:Array[Float] = array_mid.map(x=>(x._1.toFloat))
      val avgtmp3:Array[Float] = array_mid.map(x=>(x._2.toFloat))
      p += plot( DenseVector(year3), DenseVector(avgtmp3),style='-',colorcode="b",name="KUSKA站点(北纬35.28°)")
      p += plot( DenseVector(year3), DenseVector(avgtmp3),style='+',colorcode="b",name="低纬")
    })
    f.saveas("d:\\Temperuture.png")
  }
}
