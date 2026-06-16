package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
//import org.apache.flink.table.catalog.exceptions.CatalogException;
//import org.apache.flink.table.catalog.jdbc.JdbcCatalog;

import java.time.Duration;
import java.util.ArrayList;

import static org.apache.flink.table.api.Expressions.$;

class MedianAggerate extends ProcessWindowFunction<JoinedRecord, JoinedRecord, String, TimeWindow> {
	@Override
	public void process(String s, ProcessWindowFunction<JoinedRecord, JoinedRecord, String, TimeWindow>.Context context, Iterable<JoinedRecord> elements, Collector<JoinedRecord> out) throws Exception {
		ArrayList<JoinedRecord> list = new ArrayList<JoinedRecord>();
		elements.forEach(list::add);
		if (list.isEmpty()) {
			return;
		}
		String final_id = list.get(0).getId();
		String final_typename = list.get(0).getTypename();
		double final_timestamp = list.get(list.size() - 1).getTimestamp();
		list.sort((a, b) -> Double.compare(a.getHumidity(), b.getHumidity()));
		double final_humidity = list.get(list.size() / 2).getHumidity(); // take median
		double final_temperature = list.stream()
				.map(JoinedRecord::getTemperature)
				.reduce(0.0, Double::sum); // first, calcualte sum
		final_temperature /= list.size(); // then, divide to number of elements

		out.collect(new JoinedRecord(
				final_id,
				final_typename,
				final_temperature,
				final_humidity,
				final_timestamp
		));
	}
}

public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		// Initialize the stream environment settings
//		EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		tableEnv.executeSql(
				"CREATE TABLE measuring_devices (\n" +
						"    id         VARCHAR(50),\n" +
						"    typename   VARCHAR(100)\n" +
						") WITH (\n" +
						"    'connector' = 'jdbc',\n" +
						"    'url' = 'jdbc:postgresql://localhost:5432/mydatabase',\n" +
						"    'table-name' = 'measuring_devices',\n" +
						"    'username' = 'admin',\n" +
						"    'password' = 'admin123'\n" +
						");"
		);

		tableEnv.executeSql("CREATE TABLE kafka_source_table (\n" +
				"    `id` VARCHAR(100),\n" +
				"    `temperature` DOUBLE,\n" +
				"    `humidity` DOUBLE,\n" +
				"    `timestamp` DOUBLE\n" +
				") WITH (\n" +
				"    'connector' = 'kafka',\n" +
				"    'topic' = 'test_flink',\n" +
				"    'properties.bootstrap.servers' = 'localhost:9092',\n" +
				"    'properties.group.id' = 'flink-consumer-group',\n" +
				"    'scan.startup.mode' = 'earliest-offset',\n" +
				"    'format' = 'json'\n" +
				");");

		Table joined = tableEnv.sqlQuery("SELECT A.id, A.`temperature`, A.`humidity`, A.`timestamp`, B.typename\n" +
				"FROM kafka_source_table AS A, measuring_devices AS B\n" +
				"WHERE A.`id` = B.`id`");

//		joined.execute().print();

		DataStream<Row> joinedStreamRows = tableEnv.toDataStream(joined);

		DataStream<JoinedRecord> joinedStreamFields = joinedStreamRows.map(row -> new JoinedRecord(
				row.getFieldAs("id"),
				row.getFieldAs("typename"),
				row.getFieldAs("temperature"),
				row.getFieldAs("humidity"),
				row.getFieldAs("timestamp")));

		final int WINDOW_SECONDS = 60;
		DataStream<JoinedRecord> resultStream = joinedStreamFields
				.keyBy(JoinedRecord::getId)
				.window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(WINDOW_SECONDS)))
				.process(new MedianAggerate());

		resultStream.print();

		KafkaSink<JoinedRecord> sink = KafkaSink.<JoinedRecord>builder()
				.setBootstrapServers("localhost:9092")
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
						.setTopic("flink_result")
						.setKafkaValueSerializer(JoinedRecordSerializer.class)
						.build()
				)
				.build();

		resultStream.sinkTo(sink);

		env.execute("Flink Java API Skeleton");
	}
}
