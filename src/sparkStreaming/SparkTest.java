package sparkStreaming;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.google.common.base.Optional;

import scala.Tuple2;

public class SparkTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Test").setMaster("local[4]");
		JavaSparkContext cxt = new JavaSparkContext(conf);
		SQLContext sqlCtx =new SQLContext(cxt);
				
		
		
		
		testSet(cxt,sqlCtx);
	}
	public static void  testSet(JavaSparkContext ctx,SQLContext sqlCtx){
		List<String> list = Arrays.asList(
				"{\"name\":\"sunkl\"}"
				);
		JavaRDD<String> x = ctx.parallelize(list);
		sqlCtx.read().json(x).registerTempTable("s");
		System.out.println(sqlCtx.sql("select * from s").count());
		System.out.println(sqlCtx.sql("select * from s").count());
		System.out.println(sqlCtx.sql("select * from s").count());
		System.out.println(sqlCtx.sql("select * from s").count());
		System.out.println(sqlCtx.sql("select * from s").count());
		System.out.println(sqlCtx.sql("select * from s").count());
		System.out.println(sqlCtx.sql("select * from s").count());
		System.out.println(sqlCtx.sql("select * from s").count());
		sqlCtx.dropTempTable("s");
	}
}
