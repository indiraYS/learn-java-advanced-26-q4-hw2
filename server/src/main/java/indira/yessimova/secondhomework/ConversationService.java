package indira.yessimova.secondhomework;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Random;

@Slf4j
public class ConversationService extends ConversationServiceGrpc.ConversationServiceImplBase {

    private static final List<String> GREETINGS = List.of(
            "Hello",          // English
            "Hola",           // Spanish
            "Privet",         // Russian
            "As-salamu alaikum", // Arabic
            "Bonjour",        // French
            "Ciao",           // Italian
            "Hallo",          // German
            "Olá",            // Portuguese
            "Merhaba",        // Turkish
            "Konnichiwa",     // Japanese
            "Nǐ hǎo",         // Chinese
            "Annyeonghaseyo", // Korean
            "Namaste",        // Hindi
            "Sawubona"        // Zulu
    );

    private static final Random RANDOM = new Random();

    public void handleMessage(Simple.InboundMessage message, StreamObserver<Simple.OutboundMessage> responseObserver) {
        log.info("Received a new message ==> [{}]", message);

        String response = GREETINGS.get(RANDOM.nextInt(GREETINGS.size()));
        responseObserver.onNext(Simple.OutboundMessage.newBuilder()
                .setMessage(response).build());
        responseObserver.onCompleted();
    }
}
