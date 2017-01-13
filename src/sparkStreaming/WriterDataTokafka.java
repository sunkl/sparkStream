package sparkStreaming;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.netlib.util.intW;

import breeze.linalg.reshape;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import spark.DB;
import spark.Table;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class WriterDataTokafka {
	public static void main(String[] args) throws IOException, InterruptedException  {
		Producer<String, String> producer = null;
		String topic = "spark_stream_zxl_test";
//		String topic = "spark_streaming_test3";
		Properties properties = new Properties();
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		properties.put("metadata.broker.list", "kafka1.datacube.dcf.net:9092,hbase.datacube.dcf.net:9092,hive.datacube.dcf.net:9092");
		producer = new Producer<String, String>(new ProducerConfig(properties));
		
		
		File dt = new File("source/dbTable.properties");
		FileInputStream fis1 = new FileInputStream(dt);
		Properties p1 = new Properties();
		p1.load(fis1);
		fis1.close();
		Collection<Object> dbtable = p1.values();
		
		File updatecolFile = new File("source/updateCol.properties");
		FileInputStream ucfis = new FileInputStream(updatecolFile);
		Properties ucp = new Properties();
		ucp.load(ucfis);
		ucfis.close(); 
		
		LocalDate ld = LocalDate.parse("2016-07-01");
//		LocalDate ld = LocalDate.parse("2017-01-03");
		String updateCol = null;
		while(true){
			LocalDateTime ldt = LocalDateTime.now();
			for(Object temp : dbtable){
				String db = temp.toString().split("%")[0];
				String table  = temp.toString().split("%")[1];
				updateCol=ucp.getProperty(table);
				List<String> list = loadData(db,table, ld.toString(),updateCol);
				if(list.size()==0){
//					System.err.println(temp);
				}
				for (String message : list) {
					producer.send(new KeyedMessage<String, String>(topic, message));
//					System.out.println(ldt.toString()+":"+message);
				}
			}
		Thread.sleep(1000 * 30);
			
		}
//		List<String> list = loadData(DB.dcf_loan, Table.t_loan_document, "2016-05-26");
//		for (String message : list) {
//			producer.send(new KeyedMessage<String, String>(topic, message));
//		}
//		Thread.sleep(1000 * 60);
	}

	public static List<String> loadData(String db, String table, String date,String updataCol) {
		List<String> result = new LinkedList<String>();
		try {
			File f = new File("source/pkey.properties");
			FileInputStream fis = new FileInputStream(f);
			Properties p = new Properties();
			p.load(fis);
			fis.close();
			
			Class.forName("com.mysql.jdbc.Driver");
			String url = "jdbc:mysql://192.168.90.150:3306/" + db.toString()+"?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useLegacyDatetimeCode=false&serverTimezone=Asia/Shanghai&useSSL=false";
			String user = "application";
			String password = "iqx2095@0000#6969";
			
			Connection conn = DriverManager.getConnection(url, user, password);
			Statement st = conn.createStatement();
			String sql = "select * from " + db.toString() + "." + table.toString() 
//					+ " where left(" + p.getProperty(table.toString()) + " ,10) ='" + date + "' limit 5"
				+" order by "+updataCol +" desc limit 5";
					;
//			System.out.println(sql);
			ResultSet rs = st.executeQuery(sql);
			ResultSetMetaData metaData = rs.getMetaData();
			
			ObjectMapper om = new ObjectMapper();
			ObjectNode root = (ObjectNode) om.readTree("{}");
			ObjectNode on = (ObjectNode) om.readTree("{}");
			while (rs.next()) {
				for (int i = 1; i <= metaData.getColumnCount(); i++) {
					String keyName = metaData.getColumnName(i);
					String value = rs.getString(i);
					if(keyName.equals(updataCol)){
						value = "2016-07-01 00:00:00.0";
					}
					on.put(keyName, value);
				}
				root.put("dbname", db.toString());
				root.put("tablename", table.toString());
				root.put("content", on);
				result.add(root.toString());
			}
			rs.close();
			st.close();
			conn.close();
		} catch (Exception e) {
//			System.out.println(e);
		}
		
		return result;
	}

}
