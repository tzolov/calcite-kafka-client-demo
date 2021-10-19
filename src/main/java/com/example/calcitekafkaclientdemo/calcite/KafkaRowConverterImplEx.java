package com.example.calcitekafkaclientdemo.calcite;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.calcite.adapter.kafka.KafkaRowConverter;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author Christian Tzolov
 */
public class KafkaRowConverterImplEx implements KafkaRowConverter<byte[], byte[]> {
	/**
	 * Generates the row schema for a given Kafka topic.
	 *
	 * @param topicName Kafka topic name
	 * @return row type
	 */
	@Override
	public RelDataType rowDataType(final String topicName) {
		final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
		final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
		fieldInfo.add("ROWTIME", typeFactory.createSqlType(SqlTypeName.TIMESTAMP)).nullable(false);
		fieldInfo.add("MSG_PARTITION", typeFactory.createSqlType(SqlTypeName.INTEGER)).nullable(false);
//		fieldInfo.add("MSG_TIMESTAMP", typeFactory.createSqlType(SqlTypeName.TIMESTAMP)).nullable(false);
		fieldInfo.add("MSG_DATE", typeFactory.createSqlType(SqlTypeName.DATE)).nullable(false);
		fieldInfo.add("MSG_OFFSET", typeFactory.createSqlType(SqlTypeName.BIGINT)).nullable(false);
		fieldInfo.add("MSG_KEY_BYTES", typeFactory.createSqlType(SqlTypeName.VARBINARY)).nullable(true);
		fieldInfo.add("MSG_VALUE_BYTES", typeFactory.createSqlType(SqlTypeName.VARBINARY)).nullable(false);

		return fieldInfo.build();
	}

	/**
	 * Parses and reformats a Kafka message from the consumer, to align with the
	 * row schema defined as {@link #rowDataType(String)}.
	 *
	 * @param message Raw Kafka message record
	 * @return fields in the row
	 */
	@Override
	public Object[] toRow(final ConsumerRecord<byte[], byte[]> message) {
		Object[] fields = new Object[6];
		fields[0] = message.timestamp();
		fields[1] = message.partition();
		fields[2] = message.timestamp() / DateTimeUtils.MILLIS_PER_DAY;
//		fields[2] = new java.sql.Date((new Date().getTime() / DateTimeUtils.MILLIS_PER_DAY)).getTime();
		fields[3] = message.offset();
		fields[4] = message.key();
		fields[5] = message.value();

		return fields;
	}

	private String convertTime(long time){
		Date date = new Date(time);
		Format format = new SimpleDateFormat("yyyy MM dd HH:mm:ss");
		return format.format(date);
	}
}

