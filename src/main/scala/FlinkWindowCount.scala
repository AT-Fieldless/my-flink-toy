import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class AlgoClick(algoName: String, ts: Long)
class TimeStampExtractor extends AssignerWithPeriodicWatermarks[AlgoClick] {
  override def getCurrentWatermark: Watermark = {
    new Watermark(System.currentTimeMillis())
  }

  override def extractTimestamp(element: AlgoClick, previousElementTimestamp: Long): Long = {
    element.ts
  }
}

class MyProcessFunction extends AggregateFunction[AlgoClick, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: AlgoClick, accumulator: Long): Long = accumulator+1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a+b
}

class MyProcessWindowFunction extends ProcessWindowFunction[Long, (String, Long), String, TimeWindow] {

  override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[(String, Long)]): Unit = {
        val algoName = key
        val count = elements.iterator.next
        out.collect(new Tuple2[String, Long](algoName, count))
  }
}

object FlinkWindowCount {

  //用集合元素作为数据源
  def init(): List[AlgoClick] = {
    val algoClick1 = AlgoClick("algo1", System.currentTimeMillis())
    val algoClick2 = AlgoClick("algo2", System.currentTimeMillis())
    val algoClick3 = AlgoClick("algo2", System.currentTimeMillis())
    val algoClickList = List(algoClick1, algoClick2, algoClick3)
    algoClickList
  }
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironment()
    //使用eventTime作为
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.fromCollection(init()).assignTimestampsAndWatermarks(new TimeStampExtractor)
      .keyBy(_.algoName)
      .window(TumblingEventTimeWindows.of(Time.minutes(5L)))
      .aggregate(new MyProcessFunction, new MyProcessWindowFunction)
      .print()

    env.execute()
  }
}