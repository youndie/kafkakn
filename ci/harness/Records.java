// The consumer's third party (B-36): writes the records kafkakn reads, and reads them back as hex.
//
// NOT kafka-console-producer, and that is a finding rather than a preference: the console tools read
// and print TEXT, so a value that is not valid UTF-8 - the case a byte-typed consumer exists to get
// right - cannot be written by one or read back by the other without being replaced by U+FFFD. This
// is the Kafka distribution's own client, run as a program of its own; kafkakn is not on its
// classpath, so a consumer checked against it is not checked against itself.
//
//   java -cp <kafka-clients.jar>:<slf4j-api.jar> Records.java write <bootstrap> <topic>
//   java -cp ...                                   Records.java dump  <bootstrap> <topic>
//   java -cp ...                                   Records.java trickle <bootstrap> <topic> <partitions> <count> <interval-ms>
//
// The dump format is the one ConsumerTest records, one record per line:
//   <offset>/<timestamp>/<key>/<value>/<header>,<header>...   with bytes as x<hex>, null as ~
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class Records {
    /** Record i carries timestamp BASE + i * 1000, so a seek to a time has one right answer. */
    static final long BASE = 1_700_000_000_000L;
    static final int COUNT = 20;

    public static void main(String[] args) throws Exception {
        switch (args[0]) {
            case "write" -> write(args[1], args[2]);
            case "dump" -> dump(args[1], args[2]);
            case "trickle" -> trickle(args[1], args[2], Integer.parseInt(args[3]), Integer.parseInt(args[4]), Long.parseLong(args[5]));
            default -> throw new IllegalArgumentException(args[0]);
        }
    }

    static byte[] text(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    static void write(String bootstrap, String topic) throws Exception {
        Map<String, Object> config = Map.of("bootstrap.servers", bootstrap, "acks", "all");
        try (var producer = new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer())) {
            for (int i = 0; i < COUNT; i++) {
                byte[] key = text("k" + i);
                byte[] value = text("record " + i);
                List<Header> headers = new ArrayList<>();
                switch (i) {
                    case 1 -> key = null;                                                   // no key
                    case 2 -> value = null;                                                 // a tombstone
                    case 3 -> value = new byte[] {(byte) 0xff, (byte) 0xfe, 0x00, (byte) 0x80, (byte) 0xc3, 0x28}; // not UTF-8
                    case 4 -> {                                                             // duplicate names, a null value
                        headers.add(new RecordHeader("h", text("a")));
                        headers.add(new RecordHeader("h", text("b")));
                        headers.add(new RecordHeader("x", null));
                    }
                    case 5 -> {                                                             // empty, which is not null
                        key = new byte[0];
                        value = new byte[0];
                    }
                    default -> { }
                }
                producer.send(new ProducerRecord<>(topic, 0, BASE + i * 1000L, key, value, headers)).get();
            }
        }
        System.out.println("wrote " + COUNT);
    }

    /**
     * B-37: records arriving WHILE a group forms, splits and hands partitions over. Written all at once,
     * whichever member joined first would read everything before the second arrived, and a split would
     * never meet a record. Record i goes to partition i % partitions, value "t:<i>".
     */
    static void trickle(String bootstrap, String topic, int partitions, int count, long intervalMs) throws Exception {
        Map<String, Object> config = Map.of("bootstrap.servers", bootstrap, "acks", "all");
        try (var producer = new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer())) {
            for (int i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(topic, i % partitions, null, text("t" + i), text("t:" + i))).get();
                Thread.sleep(intervalMs);
            }
        }
        System.out.println("trickled " + count);
    }

    static String bytes(byte[] b) {
        return b == null ? "~" : "x" + HexFormat.of().formatHex(b);
    }

    static void dump(String bootstrap, String topic) {
        Map<String, Object> config =
            Map.of("bootstrap.servers", bootstrap, "isolation.level", "read_committed", "enable.auto.commit", "false");
        try (var consumer = new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            var partition = new TopicPartition(topic, 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long end = consumer.endOffsets(List.of(partition)).get(partition);
            // Long enough for B-70's hour of output: a deadline that ends a dump early is a count that lies.
            long deadline = System.currentTimeMillis() + 600_000;
            while (consumer.position(partition) < end && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<byte[], byte[]> r : consumer.poll(Duration.ofMillis(500))) {
                    List<String> headers = new ArrayList<>();
                    for (Header h : r.headers()) headers.add(h.key() + ":" + bytes(h.value()));
                    System.out.println(r.offset() + "/" + r.timestamp() + "/" + bytes(r.key()) + "/" + bytes(r.value()) + "/"
                        + String.join(",", headers));
                }
            }
        }
    }
}
