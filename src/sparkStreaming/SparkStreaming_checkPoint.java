package sparkStreaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import kafka.serializer.StringDecoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphOps;
import org.apache.spark.graphx.GraphXUtils;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.graphx.impl.GraphImpl;
import org.apache.spark.graphx.util.GraphGenerators;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

public class SparkStreaming_checkPoint {
	public static void main(String[] args) {
		Graphxx();
	}
	public static void Graphxx(){
		 SparkConf  sparkConf = new SparkConf().setAppName("Sunkl_Spark").setMaster("local[3]");
		 sparkConf.set("yarn.resourcemanager.address", "yarnserver.datacube.dcf.net:8032");
		 JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		 JavaPairRDD<Long, Tuple2<String, String>> users = jsc.parallelizePairs(Arrays.asList(
				 new Tuple2<Long, Tuple2<String, String>>(1L,new Tuple2<String,String>("rxin", "student")),
				 new Tuple2<Long, Tuple2<String, String>>(2L,new Tuple2<String,String>("jgonzal", "prof")),
				 new Tuple2<Long, Tuple2<String, String>>(3L,new Tuple2<String,String>("frankLin", "prof")),
				 new Tuple2<Long, Tuple2<String, String>>(4L,new Tuple2<String,String>("istoica", "studdent"))
			));
		 Graph<Tuple2<Object, Object>, Object> x = GraphGenerators.gridGraph(jsc.sc(),4,5);
		 GraphOps<Tuple2<Object, Object>, Object> go = x.ops();
		
		 System.out.println( go.inDegrees());
	}
	public static void check_point(){
		 final SparkConf  sparkConf = new SparkConf().setAppName("Sunkl_Spark");
		 sparkConf.set("yarn.resourcemanager.address", "yarnserver.datacube.dcf.net:8032");
		String zkQuoRum = "zookeeper0.datacube.dcf.net:2181,zookeeper1.datacube.dcf.net:2181,zookeeper2.datacube.dcf.net:2181";
		String brokers = "kafka1.datacube.dcf.net:9092,kafka0.datacube.dcf.net:9092";
		Map<String, Integer> topic_numberPar = new Hashtable<String,Integer>();
		topic_numberPar.put("spark_streaming_test",10);
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", brokers);
		Set<String> topicsSet = new HashSet<String>();
		topicsSet.add("spark_streaming_test");
		
		String path = "hdfs://";
		JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(path, new Function0<JavaStreamingContext>() {
			@Override
			public JavaStreamingContext call() throws Exception {
				JavaStreamingContext jsc = new JavaStreamingContext(sparkConf,new Duration(1000));
				return jsc;
			}
		});
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
		        jsc,
		        String.class,
		        String.class,
		        StringDecoder.class,
		        StringDecoder.class,
		        kafkaParams,
		        topicsSet
		);
		/*
		 *  save Steaming data -> hdfs
		 */
		/* method 1  part of the file pathName can be defined by user*/
		messages.saveAsNewAPIHadoopFiles(
				"hdfs://", 
				"后缀",
				String.class,// input key type
				String.class,// input value type
				TextOutputFormat.class,
				jsc.sparkContext().hadoopConfiguration()
		);
		/* method 2 user can define file pathName */
		messages.foreachRDD(new Function<JavaPairRDD<String,String>, Void>() {
			
			@Override
			public Void call(JavaPairRDD<String, String> v1) throws Exception {
				String path = "hdfs://";
				v1.saveAsTextFile("path/"+System.currentTimeMillis());
				return null;
			}
		});
		/*
		 * operation window 
		 */
	}
}
