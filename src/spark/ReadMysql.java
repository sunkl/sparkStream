package spark;

import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

public class ReadMysql {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("springXD_SparkStreaming").setMaster("local[10]");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		SQLContext sqlCtx = new SQLContext(ctx);
		String url = "jdbc:mysql://192.168.90.150:3306/dcf_contract?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useLegacyDatetimeCode=false&serverTimezone=Asia/Shanghai&useSSL=false";
		String[] condition = new String[]{
				" second(cs_utime)%10 = 1",
				" second(cs_utime)%10 = 2",
				" second(cs_utime)%10 = 3",
				" second(cs_utime)%10 = 4",
				" second(cs_utime)%10 = 5",
				" second(cs_utime)%10 = 6",
				" second(cs_utime)%10 = 7",
				" second(cs_utime)%10 = 8",
				" second(cs_utime)%10 = 9",
				" second(cs_utime)%10 = 0",
				" cs_utime is null "
		};
		Properties pro = new Properties();
		 pro.setProperty("user", "application");
		 pro.setProperty("password", "App@DCF#475851%");
		 pro.setProperty("driver", "com.mysql.jdbc.Driver");
		long x = sqlCtx.read().jdbc(url, "t_oc_customer_service", condition, pro).count();
		System.out.println("*****************"+x);
	}
}
