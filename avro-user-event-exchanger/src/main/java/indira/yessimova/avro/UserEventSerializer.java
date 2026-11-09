package indira.yessimova.avro;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;

public class UserEventSerializer implements Serializer<UserEvent> {

    private final SpecificDatumWriter<UserEvent> writer = new SpecificDatumWriter<>(UserEvent.SCHEMA$);

    @Override
    public byte[] serialize(String topic, UserEvent data) {
        if (data == null) return null;
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(data, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize UserEvent", e);
        }
    }
}
