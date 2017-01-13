package sparkStreaming;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;

import com.mysql.fabric.xmlrpc.base.Data;

import scala.Tuple2;

public class Test {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("sdf").setMaster("local[3]");
		JavaSparkContext ctx = new JavaSparkContext(conf);
		JavaRDD<String> rdd = ctx.textFile("hdfs://hann/rawdata/spark_temp/offlineCache/json/rawdocStandby/2016-05-26");
		System.out.println(rdd.count());
	}
	//	public static void main(String[] args) {
//		 ctx.setCheckpointDir("");
//		 Broadcast<String> x =  ctx.broadcast("");
//		 x.getValue();
//		 JavaPairRDD<String, String> y = ctx.textFile(SourceMap.getSourcePath("t_loan_document")).mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, String,String>() {
//
//			@Override
//			public Iterable<Tuple2<String, String>> call(Iterator<String> t)
//					throws Exception {
//				List<Tuple2<String, String>> result = new CopyOnWriteArrayList<Tuple2<String,String>>();
//				while(t.hasNext()){
//					String x= t.next();
//					Tuple2<String, String> tup = new Tuple2<String, String>(x,x);
//					result.add(tup);
//				}
//				return result;
//			}
//		}).reduceByKey(new Function2<String, String, String>() {
//			
//			@Override
//			public String call(String v1, String v2) throws Exception {
//				// TODO Auto-generated method stub
//				return v1+v2;
//			}
//		});
//	
//		 y.persist(StorageLevel.MEMORY_AND_DISK());
//		System.out.println(y.count());
//		
//	}
}
