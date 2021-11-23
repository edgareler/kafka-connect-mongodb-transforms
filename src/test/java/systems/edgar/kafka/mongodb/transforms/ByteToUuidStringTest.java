package systems.edgar.kafka.mongodb.transforms;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import systems.edgar.kafka.mongodb.transforms.ByteToUuidString.Value;

public class ByteToUuidStringTest {

	private static final String BASE64_UUID = "miZj0bVRTNC8FN6Rvc0mBQ==";
	private static final String DECODED_UUID = "9a2663d1-b551-4cd0-bc14-de91bdcd2605";
	private static final String TEST_NAME = "test";
	private static final int TEST_COUNT = 123;
	private static final boolean TEST_VALID = true;

	private static Schema MONGO_SCHEMA_UUID;
	private static Struct MONGO_STRUCT_UUID;
	private static Map<String, Object> MONGO_MAP_UUID;

	private static final Map<String, Object> PROPS = new HashMap<>();

	@BeforeAll
	static void configurePayloads() {
		MONGO_SCHEMA_UUID = SchemaBuilder.struct()
				.field("id", Schema.BYTES_SCHEMA)
				.field("name", Schema.STRING_SCHEMA)
				.field("count", Schema.INT32_SCHEMA)
				.field("valid", Schema.BOOLEAN_SCHEMA)
				.build();

		byte[] encodedId = Base64.getDecoder().decode(BASE64_UUID);

		MONGO_STRUCT_UUID = new Struct(MONGO_SCHEMA_UUID)
				.put("id", encodedId)
				.put("name", TEST_NAME)
				.put("count", TEST_COUNT)
				.put("valid", TEST_VALID);

		MONGO_MAP_UUID = new LinkedHashMap<>();
		MONGO_MAP_UUID.put("id", encodedId);
		MONGO_MAP_UUID.put("name", TEST_NAME);
		MONGO_MAP_UUID.put("count", TEST_COUNT);
		MONGO_MAP_UUID.put("valid", TEST_VALID);

		PROPS.put(ByteToUuidString.FIELD_NAME, "id");
	}

	@Test
	@DisplayName("transform base64 byte value to UUID string with schema")
	void byteToUuidWithSchema() {
		SourceRecord record = new SourceRecord(
				null,
				null,
				"test-kafka-topic",
				0,
				MONGO_SCHEMA_UUID,
				MONGO_STRUCT_UUID
		);

		Value<SourceRecord> transform = new Value<>();
		transform.configure(PROPS);

		Struct transformedRecord = (Struct) transform.apply(record).value();

		assertAll(
				() -> assertEquals(transformedRecord.getString("id"), DECODED_UUID),
				() -> assertEquals(transformedRecord.getString("name"), TEST_NAME),
				() -> assertEquals(transformedRecord.getInt32("count"), TEST_COUNT),
				() -> assertEquals(transformedRecord.getBoolean("valid"), TEST_VALID)
		);

		transform.close();
	}

	@Test
	@DisplayName("transform base64 byte value to UUID string without schema")
	@SuppressWarnings("unchecked")
	void byteToUuidWithoutSchema() {
		SourceRecord record = new SourceRecord(
				null,
				null,
				"test-kafka-topic",
				0,
				null,
				MONGO_MAP_UUID
		);

		Value<SourceRecord> transform = new Value<>();
		transform.configure(PROPS);

		Map<String, Object> transformedRecord = (Map<String, Object>) transform.apply(record).value();

		assertAll(
				() -> assertEquals(transformedRecord.get("id"), DECODED_UUID),
				() -> assertEquals(transformedRecord.get("name"), TEST_NAME),
				() -> assertEquals(transformedRecord.get("count"), TEST_COUNT),
				() -> assertEquals(transformedRecord.get("valid"), TEST_VALID)
		);

		transform.close();
	}

}
