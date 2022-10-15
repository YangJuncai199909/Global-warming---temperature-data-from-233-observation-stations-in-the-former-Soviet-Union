# Global-warming---temperature-data-from-233-observation-stations-in-the-former-Soviet-Union
# 本次大数据技术实训个人部分主要针对期末大作业第二题进行延伸和拓展。通过查阅前苏联233个观测站点的经纬度数据，选择了纬度最高和最低以及纬度和首都莫斯科相近的三个站点作为实验对象。通过计算该三个站点从1881年到2001年每年8月份的平均温度来体现俄罗斯地区的温度总体变化趋势。
# 3.1数据的预处理
将上述三个观测站点的数据信息放入一个文件夹中，通过textFile操作导入。
后对数据进行预处理（过滤和分割数据集）。
val pre_data1 = sc.textFile("C:\\Users\\yangjuncai\\Desktop\\大学学习\\大三小学期\\大数据基础实训\\数据文件")
//数据格式处理，将三个空格和两个空格都置换成一个空格，并切分数据。
val fields = pre_data1.map(_.trim.replace("   ", " ")
  .replace("  ", " ").split(" "))
//保留15个数据域的数据，并且符合QR为有观测数据和温度值不为999.9。
//6\8\10位标志位不能为999，对数据用filter进行过滤
val pre_ann1 = fields.filter(_.length == 15).filter(_(2)=="8")
  .filter(_(5)!="999").filter(_(6)!="9")
因为选取的是每年8月份的平均温度作为观测值，所以对每年的8月份每日气温进行求平均运算。
val pre_ann_map1 = pre_ann1.map(f=>(((f(1),f(2).toInt),f(0)),f(5).toDouble))
val Mean_Temp: RDD[(((String, Int), String), Iterable[Double])] = pre_ann_map1.groupByKey().sortByKey()
val pre_ann_mean2: RDD[(((String, Int), String), Double)] = Mean_Temp
  .map(x=>(x._1,x._2.reduce(_+_) / x._2.size))
val un = pre_ann_mean2.map(x=>(x._1._2,(x._1._1._1,x._2))).groupByKey()
求完平均之后对三个站点的数据进行进行剥离和整合。
3.2数据的剥离和整合
因为数据最后是要运用breeze和breez_viz库对数据进行图像绘制的，但是breeze的plot函数库只支持稠密向量(DenseVector)和稠密矩阵(DenseMatrix)形式，所以需要将得到的RDD形式的数据转换成稠密向量的形式。我的思路如下：



以最高纬度站点的数据剥离和整合为例，首先对站点ID信息进行分离和过滤。然后想将RDD通过collect直接转换成数组，但是我们之前对观测点进行整合时采用的是groupByKey方法，第一项为ID，第二项为一个迭代器，呈Iterable[(String, Double)类型，如果直接使用collect( )不能将迭代器中的数据拿出来。通过对数据类型的查看，将RDD转换成列表（list）类型，再把list中的值通过foreach循环输出到数组中，将年份信息和温度信息分别储存到两个数组中，在将两个数组的值依次通过（x,y）输出。
val separation_1 = un.filter(_._1.toInt==20674)
val list1: List[Iterable[(String, Double)]] = separation_1.map(x => x._2).collect().toList
val Highest_Latitude = list1.foreach(x=>{
  val array_high: Array[(String, Double)] = x.toArray
  val year1:Array[Float] = array_high.map(x=>(x._1.toFloat))
  val avgtmp1:Array[Float] = array_high.map(x=>(x._2.toFloat))
4图像绘制
依据breeze绘图库的用法，通过figure函数和plot函数对数据进行可视化输出。一开始我们的年份信息是string类型，温度信息是double类型，通过plot输出时报错，显示隐式错误。通过按住Ctrl+点击plot的方法查看plot函数的源代码，发现前后两个x,y值必须是相同类型。所以统一修改成Float类型。
//plot函数前后必须类型一致
p += plot( DenseVector(year1), DenseVector(avgtmp1),style='-',colorcode="r",name="OSTROV DIKSON站点(北纬73.5°)")
p += plot( DenseVector(year1), DenseVector(avgtmp1),style='+',colorcode="r",name="高纬")
p.xlabel = "年份"
p.ylabel = "8月份平均温度"
p.title = ("不同纬度温度变化趋势图")
这里需要注意的是，数组类型的year1和avgtmp1需要通过DenseVector函数将数组类型转化成稠密向量类型才可用plot函数进行输出。plot函数具体用法如下：
def plot[X, Y, V](
    x: X,
    y: Y,
    style: Char = '-',
    colorcode: String = null,
    name: String = null,
    lines: Boolean = true,
    shapes: Boolean = false,
    labels: (Int) => String = null.asInstanceOf[Int => String],
    tips: (Int) => String = null.asInstanceOf[Int => String])(
    implicit xv: DomainFunction[X, Int, V],
    yv: DomainFunction[Y, Int, V],
    vv: V => Double): Series = new Series {
  require(xv.domain(x) == yv.domain(y), "Domains must match!")
为了将三个观测点的图像都显示在一张图中，且有对比度，我只有一个p变量进行点集的累积，最够只输出一副图片。为了呈现一种离散的形式，我在每个基站的折现图的转折点处通过“+”符号将数据点标出。最后显示图例的legend函数和matlab中的一般用法有很大的差别。通过查询legend函数在breeze库中的源码得知，legend后只能接Boolean类型，判断为true则开启legend函数自动将plot函数内部定义的name输出为图例，如果没有自己设置name的话，默认输出SeriousX。所以一开局先声明以下操作：
val f=Figure()
val p = f.subplot(0)
p.legend=(true)
最后得到的图像结果还算是近乎人意，图像结果如下图4-1所示：

5结果分析
通过在地球在线网站上查询经纬度，得到了三个观测点的大致位置信息。如下：
OSTROV DIKSON(73.50N,80.40E)

俄罗斯最北部的村庄，一年中8个月是被冰雪所覆盖，全年平均气温大概在0摄氏度左右。地点靠近北冰洋，受洋流影响较大。
KURGAN(55.47N,65.40E)

地理位置处于大陆中部地区，深处内陆，受海洋季风等影响较小，能够较好地反映内陆地区的气温变化情况。
KUSKA(35.28N,62.35E)

地理位置处于现今阿富汗和伊朗交接地带，处于中纬度地带，能够较好的反映战争以及石油化工对气温及环境的影响。
从以上三个点以及气温折线图可以看出，在1982年左右，气温开始逐步呈上升态势，以伊朗和阿富汗边境的观测点的变化最为明显。在1995年左右，气温上升趋势增幅上升，年平均气温总体呈上升态势，三点均是如此。可以纵向得出结论，在不同纬度地区，均呈现年化气温上升，从一定程度上体现了全球气候变暖的总体趋势。
