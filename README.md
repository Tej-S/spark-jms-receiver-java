# spark-jms-receiver-java

Reliable Spark Streaming receiver in Java with a JMS 1.1 source.

This project is based on the synchronous JMS receiver written in Scala in [tbfenet/spark-jms-receiver](https://github.com/tbfenet/spark-jms-receiver).

The `SynchronousJmsReceiver` has been rewritten in Java with some modifications.
The biggest difference is that this receiver tries to simulate the block generation approach of an unreliable receiver, i.e. blocks are generated based on a fixed block interval milliseconds setting.
The original behavior has more configuration options such as `batchSize`, `maxBatchAge`, and `maxWait`.