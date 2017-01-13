package spark;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;

import kafka.serializer.StringDecoder;

import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.graphx.Graph;
import org.apache.spark.mllib.fpm.PrefixSpan.Prefix;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.util.MutableURLClassLoader;

import com.google.common.base.Optional;

import scala.Tuple2;
/**
 * 
 * @author zfx
 *
 */
public class SparkStream implements Serializable{
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("springXD_SparkStreaming").setMaster("local[1]");
		
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaStreamingContext jsc = new JavaStreamingContext(ctx, new Duration(100));
//		jsc.checkpoint("file:/D:/SparkStreamingCheckpoint");
//		jsc.checkpoint("hdfs:/hann/rawdata/spark_temp/2016-08-02/rawdata/spark_temp/sparkStreamingCache");
		jsc.checkpoint("file:/home/sunkl/checkpoint");
		String brokers = "kafka1.datacube.dcf.net:9092,hbase.datacube.dcf.net:9092,hive.datacube.dcf.net:9092";
		//String brokers = "192.168.11.134:9092,192.168.11.133:9092,192.168.11.132:9092";
		
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", brokers);
		Set<String> topicsSet = new HashSet<String>();
		topicsSet.add("spark_streaming_test2");
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jsc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

		messages.checkpoint(new Duration(100));
		JavaPairDStream<String, Integer> s1 = messages.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, Integer>> call(Tuple2<String, String> t) throws Exception {

				String[] cols = t._2.split(" ");
				List<String> list = Arrays.asList(cols);
				List<Tuple2<String, Integer>> result = new CopyOnWriteArrayList<Tuple2<String, Integer>>();
				for (String temp : list) {
					result.add(new Tuple2<String, Integer>(temp, 1));
				}
				return result;
			}
		});
		 
		 s1.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
			private static final long serialVersionUID = 1L;
			@Override
			 public Optional<Integer> call(List<Integer> v1, Optional<Integer> v2) throws Exception {
				 Integer result = 0;
				
				 for(Integer temp : v1){
					 result += temp;
				 }
				 if(v2.isPresent()){
					 result += v2.get();
				 }
				 return Optional.fromNullable(result);
			 }
		 	}).mapToPair(new PairFunction<Tuple2<String,Integer>, String,Integer>() {
				private static final long serialVersionUID = 1L;
				@Override
				public Tuple2<String, Integer> call(Tuple2<String, Integer> t) throws Exception {
					
					return t;
				}
			}).print();
 
		jsc.start();
		jsc.awaitTermination();
	}

	/*public static void Steaming() {
	}*/

}