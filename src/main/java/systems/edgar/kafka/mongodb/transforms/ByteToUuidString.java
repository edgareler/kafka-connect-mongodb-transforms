package systems.edgar.kafka.mongodb.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMapOrNull;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStructOrNull;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public abstract class ByteToUuidString<R extends ConnectRecord<R>> implements Transformation<R> {
	static String FIELD_NAME = "field.name";
	private static final String PURPOSE = "transform byte[] to UUID string";
    static final ConfigDef CONFIG_DEF = new ConfigDef().define(FIELD_NAME, Type.STRING, Importance.HIGH, PURPOSE);
    private String fieldName;

    @SuppressWarnings("serial")
	private static final Map<Schema.Type, Schema> TYPES_MAP = new HashMap<Schema.Type, Schema>() {{
    	put(Schema.Type.INT8, Schema.INT8_SCHEMA);
    	put(Schema.Type.INT16, Schema.INT16_SCHEMA);
    	put(Schema.Type.INT32, Schema.INT32_SCHEMA);
    	put(Schema.Type.INT64, Schema.INT64_SCHEMA);
    	put(Schema.Type.FLOAT32, Schema.FLOAT32_SCHEMA);
    	put(Schema.Type.FLOAT64, Schema.FLOAT64_SCHEMA);
    	put(Schema.Type.BOOLEAN, Schema.BOOLEAN_SCHEMA);
    	put(Schema.Type.STRING, Schema.STRING_SCHEMA);
    	put(Schema.Type.BYTES, Schema.BYTES_SCHEMA);
    }};

	@Override
	public R apply(R record) {
		if (operatingSchema(record) == null) {
			return applySchemaless(record);
		}

		return applyWithSchema(record);
	}

	private R applySchemaless(R record) {
		final Map<String, Object> value = requireMapOrNull(operatingValue(record), PURPOSE);

		if (value == null) {
			return newRecord(record, null, null);
		}

		final Map<String, Object> newValue = new HashMap<>(value);

		newValue.put(fieldName, byteToUuid((byte[])value.get(fieldName)));

		return newRecord(record, null, newValue);
	}

	private R applyWithSchema(R record) {
		final Struct value = requireStructOrNull(operatingValue(record), PURPOSE);

		if (value == null) {
			return newRecord(record, null, null);
		}

		Schema schema = operatingSchema(record);

		final SchemaBuilder builder = SchemaUtil.copySchemaBasics(
				schema,
				SchemaBuilder.struct()
		);

		for (Field field : value.schema().fields()) {
			if (field.name().equals(fieldName)) {
				builder.field(fieldName, Schema.STRING_SCHEMA);
			} else {
				builder.field(field.name(), TYPES_MAP.get(field.schema().type()));
			}
		}

		Schema newSchema = builder.build();
		final Struct newValue = new Struct(newSchema);

		for (Field field : value.schema().fields()) {
			if (field.name().equals(fieldName)) {
				newValue.put(fieldName, byteToUuid(value.getBytes(fieldName)));
			} else {
				newValue.put(field.name(), value.get(field));
			}
		}

		return newRecord(record, newSchema, newValue);
	}

	@Override
	public void configure(Map<String, ?> configs) {
		final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
		fieldName = config.getString(FIELD_NAME);
	}

	@Override
	public ConfigDef config() {
		return CONFIG_DEF;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	private String byteToUuid(byte[] input) {
		ByteBuffer bb = ByteBuffer.wrap(input);
		long high = bb.getLong();
		long low = bb.getLong();
		UUID uuid = new UUID(high, low);

		return uuid.toString();
	}

	protected abstract Schema operatingSchema(R record);

	protected abstract Object operatingValue(R record);

	protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

	public static class Key<R extends ConnectRecord<R>> extends ByteToUuidString<R> {
		@Override
		protected Schema operatingSchema(R record) {
			return record.keySchema();
		}

		@Override
		protected Object operatingValue(R record) {
			return record.key();
		}

		@Override
		protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
			return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
		}
	}

	public static class Value<R extends ConnectRecord<R>> extends ByteToUuidString<R> {
		@Override
		protected Schema operatingSchema(R record) {
			return record.valueSchema();
		}

		@Override
		protected Object operatingValue(R record) {
			return record.value();
		}

		@Override
		protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
			return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
		}
	}
}
