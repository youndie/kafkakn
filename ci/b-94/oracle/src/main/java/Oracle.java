// B-94: Confluent's KafkaJsonSchemaSerializer and KafkaJsonSchemaDeserializer, on bytes, with no broker.
//
//   Oracle read  <registry> <topic> <lines>          each line "<hex>\t...": the value the deserializer reads,
//                                                     validating it against its schema, as one JSON line
//   Oracle write <registry> <topic> <schema> <lines> each line "...\t<json>": that JSON under that schema, as the
//                                                     serializer writes it, one hex line
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

public class Oracle {
    public static void main(String[] args) throws Exception {
        String registry = args[1];
        String topic = args[2];
        HexFormat hex = HexFormat.of();
        ObjectMapper mapper = new ObjectMapper();
        switch (args[0]) {
            case "read" -> {
                try (KafkaJsonSchemaDeserializer<JsonNode> deserializer = new KafkaJsonSchemaDeserializer<>()) {
                    // Validated against the schema its id names: a payload that does not match it is an error here.
                    deserializer.configure(Map.of("schema.registry.url", registry, "json.fail.invalid.schema", true), false);
                    for (String line : Files.readAllLines(Path.of(args[3]))) {
                        if (line.isBlank()) continue;
                        Object read = deserializer.deserialize(topic, hex.parseHex(line.split("\t")[0]));
                        System.out.println(mapper.writeValueAsString(read));
                    }
                }
            }
            case "write" -> {
                JsonSchema schema = new JsonSchema(Files.readString(Path.of(args[3])).trim());
                try (KafkaJsonSchemaSerializer<Object> serializer = new KafkaJsonSchemaSerializer<>()) {
                    serializer.configure(Map.of("schema.registry.url", registry, "json.fail.invalid.schema", true), false);
                    List<String> lines = Files.readAllLines(Path.of(args[4]));
                    for (String line : lines) {
                        if (line.isBlank()) continue;
                        JsonNode payload = mapper.readTree(line.split("\t")[1]);
                        byte[] written = serializer.serialize(topic, JsonSchemaUtils.envelope(schema, payload));
                        System.out.println(hex.formatHex(written));
                    }
                }
            }
            default -> throw new IllegalArgumentException("read or write, not " + args[0]);
        }
    }
}
