package indira.yessimova.secondhomework;

import io.grpc.ServerBuilder;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        int port = Integer.parseInt(System.getenv().getOrDefault("GRPC_SERVER_PORT", "8089"));
        var server = ServerBuilder.forPort(port)
                .addService(new ConversationService())
                .build()
                .start();

        server.awaitTermination();
    }
}
