package sparkStreaming;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.deploy.worker.Sleeper;
import org.netlib.lapack.Slabad;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafKaProducer {
	Producer<String, String> producer = null;
	String topic = "spark_streaming_test";
	Properties properties = new Properties();

	public Producer<String, String> Init(String topic) {
		this.topic = topic;
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		
//		properties.put("metadata.broker.list", "kafka1.datacube.dcf.net:9092,kafka0.datacube.dcf.net:9092");
		properties.put("metadata.broker.list", "kafka1.datacube.dcf.net:9092,hbase.datacube.dcf.net:9092,hive.datacube.dcf.net:9092");
		producer = new Producer<String, String>(new ProducerConfig(properties));
		return producer;
	}
	public static String genJson(long i) throws JsonProcessingException, IOException{
		ObjectMapper om = new ObjectMapper();
		ObjectNode on = (ObjectNode) om.readTree("{}");
		on.put("name", "sunkl"+1);
		if(i%2==0){
			on.put("sex","boy");
		}else{
			on.put("sex", "girl");
		}
		on.put("age",i%100);
		return on.toString();
	}
	public static void main(String[] args) throws JsonProcessingException, IOException {
		KafKaProducer x = new KafKaProducer();
//		x.Init("spark_streaming_test2");
		x.Init("spark_z_testing");
		/*
		 * for (int i = 0; true; i++) { String ch =i+""; x.producer.send(new
		 * KeyedMessage<String, String>(x.topic, ch)); }
		 */
		long i = 1;
		while (true) {
			i++; 
			String message = genJson(i);
			x.producer.send(new KeyedMessage<String, String>(x.topic, message));
			System.out.println(i);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
