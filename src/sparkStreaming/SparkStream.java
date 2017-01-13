package sparkStreaming;

import java.io.IOException;
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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.graphx.Graph;
import org.apache.spark.mllib.fpm.PrefixSpan.Prefix;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.In;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.util.MutableURLClassLoader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Optional;

import scala.Tuple2;

public class SparkStream {
	public static void main(String[] args) throws JsonProcessingException, IOException {
		SparkConf sparkConf = new SparkConf()
		.set("spark.sql.shuffle.partitions", "5")
		.setAppName("springXD_SparkStreaming").setMaster("local[8]"); 
		final JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		SQLContext sqlCtx = new SQLContext(ctx);
		
		 List<String> list = Arrays.asList(KafKaProducer.genJson(1));
		sqlCtx.read().json(ctx.parallelize(list)).registerTempTable("student");;
		sqlCtx.sql("select * from student").show();
		
		JavaStreamingContext jsc = new JavaStreamingContext(ctx, Durations.seconds(1));
		jsc.checkpoint("file:/home/sunkl/checkpoint");
		
		String zkQuoRum = "zookeeper0.datacube.dcf.net:2181,zookeeper1.datacube.dcf.net:2181,zookeeper2.datacube.dcf.net:2181";
		String brokers = "kafka1.datacube.dcf.net:9092,hbase.datacube.dcf.net:9092,hive.datacube.dcf.net:9092";
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", brokers);
		Set<String> topicsSet = new HashSet<String>();
		topicsSet.add("spark_z_testing");
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jsc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
		messages.checkpoint(new Duration(1000));
		JavaDStream<String> sStream =  messages.map(new Function<Tuple2<String,String>, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String call(Tuple2<String, String> v1) throws Exception {
				return v1._2;
			}
		});
		sStream.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public JavaRDD<String> call(JavaRDD<String> v1) throws Exception {
				long len =v1.count();
				if(len != 0){
					sqlCtx.read().json(v1).registerTempTable("s1");
					sqlCtx.sql("select * from student union select * from s1").registerTempTable("student");;
					System.out.println("startLen**********:"+len);
				
				}
				System.out.println("tableCount:"+sqlCtx.sql("select *  from student").count());
				return v1;
			}
		}).print();;
		jsc.start();
		jsc.awaitTermination();
	}

	public static void Steaming() {
	}

}
