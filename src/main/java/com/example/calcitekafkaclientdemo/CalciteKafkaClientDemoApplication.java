package com.example.calcitekafkaclientdemo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Properties;

import org.apache.calcite.jdbc.CalciteConnection;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CalciteKafkaClientDemoApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(CalciteKafkaClientDemoApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		String model = "inline:" +
				"{\n" +
				"  \"version\": \"1.0\",\n" +
				"  \"defaultSchema\": \"KAFKA\",\n" +
				"  \"schemas\": [\n" +
				"    {\n" +
				"    \"name\": \"KAFKA\",\n" +
				"    \"tables\": [\n" +
				"      {\n" +
				"        \"name\": \"USER_TITLES\",\n" +
				"        \"factory\": \"org.apache.calcite.adapter.kafka.KafkaTableFactory\",\n" +
//				"        \"factory\": \"com.example.calcitekafkaclientdemo.calcite.KafkaTableFactory2\",\n" +
				"        \"stream\": { \"stream\": true },\n" +
				"        \"operand\": {\n" +
				"          \"bootstrap.servers\": \"localhost:9092\",\n" +
//				"          \"topic.name\": \"user-titles\",\n" +
				"          \"topic.name\": \"my-cloud-events\",\n" +
				"          \"row.converter\": \"com.example.calcitekafkaclientdemo.calcite.KafkaRowConverterImplEx\",\n" +
				"          \"consumer.params\": {\n" +
				"            \"group.id\": \"calcite-ut-consumer\",\n" +
				"            \"key.deserializer\": \"org.apache.kafka.common.serialization.ByteArrayDeserializer\",\n" +
				"            \"value.deserializer\": \"org.apache.kafka.common.serialization.ByteArrayDeserializer\"\n" +
				"          }\n" +
				"        }\n" +
				"      }\n" +
				"    ]\n" +
				"    }\n" +
				"  ]\n" +
				"}";
		Properties info = new Properties();
		info.put("model", model);

		Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
		final CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

		final String sql3 = "SELECT STREAM \n" +
				"  FLOOR(\"ROWTIME\" to HOUR) as \"ROWTIME\",\n" +
				"  \"MSG_KEY_BYTES\",\n" +
				"  COUNT(*) as C\n" +
				"FROM \"KAFKA\".\"USER_TITLES\"\n" +
				"GROUP BY FLOOR(\"ROWTIME\" to HOUR), \"MSG_KEY_BYTES\"";
		final String sql2 = "SELECT STREAM HOP_START(MSG_DATE, INTERVAL '1' HOUR, INTERVAL '3' HOUR),\n" +
				"  HOP_END(MSG_DATE, INTERVAL '1' HOUR, INTERVAL '3' HOUR),\n" +
				"  COUNT(*)\n" +
				"FROM \"KAFKA\".\"USER_TITLES\"\n" +
				"GROUP BY HOP(MSG_DATE, INTERVAL '1' HOUR, INTERVAL '3' HOUR)";

		final String sql = "SELECT STREAM * FROM \"KAFKA\".\"USER_TITLES\"";
			//"  GROUP BY FLOOR(ROWTIME TO HOUR), MSG_KEY_BYTES";
		final String sql4 = "SELECT STREAM " +
				"    FLOOR(ROWTIME to HOUR) as ROWTIME2, " +
				"    MSG_KEY_BYTES, " +
				"    COUNT(*) AS C " +
				"  FROM \"KAFKA\".\"USER_TITLES\"" +
				"  GROUP BY FLOOR(ROWTIME to HOUR), MSG_KEY_BYTES";

		final String sql5 = "SELECT STREAM " +
				"    FLOOR(ROWTIME to HOUR) as ROWTIME, " +
				"    MSG_PARTITION," +
				"    COUNT(*) AS C " +
				"  FROM \"KAFKA\".\"USER_TITLES\"" +
				"  GROUP BY FLOOR(ROWTIME to HOUR), MSG_PARTITION ";

		final String sql6 =
				"SELECT STREAM " +
				"  HOP_START(ROWTIME, INTERVAL '1' HOUR, INTERVAL '2' HOUR) as HS,\n" +
				"  HOP_END(ROWTIME, INTERVAL '1' HOUR, INTERVAL '2' HOUR) as HE,\n" +
				"  COUNT(*) as C\n" +
				"FROM \"KAFKA\".\"USER_TITLES\"\n" +
				"GROUP BY HOP(ROWTIME, INTERVAL '1' HOUR, INTERVAL '2' HOUR), MSG_PARTITION";

		final String sql7 = "SELECT STREAM * FROM \"KAFKA\".\"USER_TITLES\"";

		final PreparedStatement statement = calciteConnection.prepareStatement(sql7);
		final ResultSet resultSet = statement.executeQuery();

		ResultSetMetaData metadata = resultSet.getMetaData();
		while (resultSet.next()) {
			for (int i = 1; i <= metadata.getColumnCount(); i++) {
				System.out.print(metadata.getColumnLabel(i) + "=" + resultSet.getString(i) + ",");
			}
			System.out.println();
		}
		connection.close();
		calciteConnection.close();
	}
}
