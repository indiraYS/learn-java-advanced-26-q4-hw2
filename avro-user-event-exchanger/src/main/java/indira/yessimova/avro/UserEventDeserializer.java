package indira.yessimova.avro;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.serialization.Deserializer;

public class UserEventDeserializer implements Deserializer<UserEvent> {

    private final SpecificDatumReader<UserEvent> reader = new SpecificDatumReader<>(UserEvent.SCHEMA$);

    @Override
    public UserEvent deserialize(String topic, byte[] data) {
        if (data == null) return null;
        try {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
            return reader.read(null, decoder);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize UserEvent", e);
        }
    }
}
