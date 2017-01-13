package sparkStreaming;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
public class KafKaCustomer{
	public static void main(String[] args) {
//		Consumer c1 = new Consumer("spark_streaming_test");
//		c1.start();
	}
}
class Consumer  extends Thread{
//	private final ConsumerConnector consumer;
//	  private final String topic;
	  
//	  public Consumer(String topic)
//	  {
//	    consumer = sparkStreaming.Consumer.createJavaConsumerConnector(
//	            createConsumerConfig());
//	    this.topic = topic;
//	  }

	  private static ConsumerConfig createConsumerConfig()
	  {
	    Properties props = new Properties();
	    props.put("zookeeper.connect", "zookeeper0.datacube.dcf.net:2181,zookeeper1.datacube.dcf.net:2181,zookeeper2.datacube.dcf.net:2181");
	    props.put("group.id", "id");
	    props.put("zookeeper.session.timeout.ms", "1000");
	    props.put("zookeeper.sync.time.ms", "200");
	    props.put("auto.commit.interval.ms", "1000");

	    return new ConsumerConfig(props);

	  }
	 
	  public void run() {
//	    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
//	    topicCountMap.put(topic, new Integer(1));
//	    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
//	    KafkaStream<byte[], byte[]> stream =  consumerMap.get(topic).get(0);
//	    ConsumerIterator<byte[], byte[]> it = stream.iterator();
//	    while(it.hasNext())
//	      System.out.println(new String(it.next().message()));
//	    try {
//			this.wait(200);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	  }
}
