import java.util.concurrent.TimeUnit

import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, HConnectionManager, HTableInterface, Result, Table}
import org.apache.flink.api.scala._

import scala.concurrent.{ExecutionContext, Future}

/**
 * An implementation of the 'AsyncFunction' that sends requests and sets the callback.
 */
class AsyncHbaseRequest extends RichAsyncFunction[String, Result] {
  /** The database specific client that can issue concurrent requests with callbacks */
  private var client: Table = _

  /** The context used for the future callbacks */
  implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())

  override def open(parameters: Configuration): Unit = {
    val configuration = HBaseConfiguration.create()
    val hConnection = ConnectionFactory.createConnection(configuration)
    client = hConnection.getTable(TableName.valueOf("test"))
    println("open asyncHbaseRequest")
  }

  override def close(): Unit = {
    println("close asyncHbaseRequest")
    client.close()
  }

  override def asyncInvoke(input: String, resultFuture: ResultFuture[Result]): Unit = {
    print("miao")
    val get = new Get(input.getBytes())
    val columnFamily = "stat"
    val column = "value"
    get.addColumn(columnFamily.getBytes(), column.getBytes())
    // issue the asynchronous request, receive a future for the result
    val resultFutureRequested: Future[Result] = Future{
      client.get(get)
    }

    // set the callback to be executed once the request by the client is complete
    // the callback simply forwards the result to the result future
    resultFutureRequested.onSuccess {
      case result: Result => resultFuture.complete(Iterable(result))
    }
  }
}


object FlinkAsyncIO {
  //用集合元素作为数据源
  def init(): List[String] = {
    val itemId1 = "123"
    val itemId2 = "456"
    val itemId3 = "789"
    val itemList = List(itemId1, itemId2, itemId3)
    itemList
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironment()
    val dataStream = env.fromCollection(init())
    val resultStream = AsyncDataStream.unorderedWait(dataStream, new AsyncHbaseRequest(), 1000, TimeUnit.MILLISECONDS, 100)
      .print()
    env.execute()
  }
}
