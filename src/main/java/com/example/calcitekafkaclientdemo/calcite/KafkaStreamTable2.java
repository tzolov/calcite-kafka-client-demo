/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example.calcitekafkaclientdemo.calcite;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.Properties;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.kafka.KafkaTableOptions;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.AbstractEnumerable2;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.checkerframework.checker.nullness.qual.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * A table that maps to an Apache Kafka topic.
 *
 * <p>Currently only {@link KafkaStreamTable2} is
 * implemented as a STREAM table.
 */
public class KafkaStreamTable2 implements ScannableTable, StreamableTable {
	final KafkaTableOptions tableOptions;

	KafkaStreamTable2(final KafkaTableOptions tableOptions) {
		this.tableOptions = tableOptions;
	}

	@Override
	public Enumerable<@Nullable Object[]> scan(final DataContext root) {

		return new AbstractEnumerable2<@Nullable Object[]>() {
			@Override
			public Iterator<@Nullable Object[]> iterator() {
				Properties consumerConfig = new Properties();
				consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
						tableOptions.getBootstrapServers());
				//by default it's <byte[], byte[]>
				consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
						"org.apache.kafka.common.serialization.ByteArrayDeserializer");
				consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
						"org.apache.kafka.common.serialization.ByteArrayDeserializer");

				if (tableOptions.getConsumerParams() != null) {
					consumerConfig.putAll(tableOptions.getConsumerParams());
				}
				Consumer consumer = new KafkaConsumer<>(consumerConfig);
				consumer.subscribe(Collections.singletonList(tableOptions.getTopicName()));

				return new MyIterator<>(consumer);
			}
		};
	}

	private class MyIterator<K, V> implements Iterator<Object[]> {
		//runtime
		private Consumer consumer;
		final Deque<ConsumerRecord<K, V>> bufferedRecords = new ArrayDeque<>();
		@Nullable ConsumerRecord<K, V> curRecord = null;

		private MyIterator(Consumer consumer) {
			this.consumer = consumer;
		}

		@Override
		public boolean hasNext() {
			return true;
		}

		@Override
		public Object[] next() {
			while (bufferedRecords.isEmpty()) {
				ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord record : records) {
					bufferedRecords.add(record);
				}
			}

			curRecord = bufferedRecords.removeFirst();
			return tableOptions.getRowConverter().toRow(requireNonNull(curRecord, "curRecord"));
		}
	}

	@Override
	public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
		return tableOptions.getRowConverter().rowDataType(tableOptions.getTopicName());
	}

	@Override
	public Statistic getStatistic() {
		return Statistics.of(100d, ImmutableList.of(),
				RelCollations.createSingleton(0));
	}

	@Override
	public boolean isRolledUp(final String column) {
		return false;
	}

	@Override
	public boolean rolledUpColumnValidInsideAgg(final String column, final SqlCall call,
			final @Nullable SqlNode parent,
			final @Nullable CalciteConnectionConfig config) {
		return false;
	}

	@Override
	public Table stream() {
		return this;
	}

	@Override
	public Schema.TableType getJdbcTableType() {
		return Schema.TableType.STREAM;
	}
}
