package spark;

import java.io.IOException;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import scala.tools.nsc.doc.model.diagram.ObjectNode;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.mysql.jdbc.Connection;

public class JsonPat {
	public static void main(String[] args) throws JsonProcessingException, IOException { 
		ObjectMapper om = new ObjectMapper();
		JsonNode jackson = om.readTree("{\"name1\":\"df1\",\"name2\":\"df2\",\"name3\":\"df3\",\"name4\":{\"name5\":\"df5\"}}");
		System.out.println(jackson);
		
		DocumentContext jsonPath = JsonPath.parse("{\"name1\":\"df1\",\"name2\":\"df2\",\"name3\":\"df3\",\"name4\":{\"name5\":\"df5\"}}");
		System.out.println(jsonPath);
	}
}
