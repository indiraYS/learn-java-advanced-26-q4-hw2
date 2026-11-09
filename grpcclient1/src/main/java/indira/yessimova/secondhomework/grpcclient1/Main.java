package indira.yessimova.secondhomework.grpcclient1;


import indira.yessimova.secondhomework.ConversationServiceGrpc;
import indira.yessimova.secondhomework.Simple;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class Main {
    public static void main(String[] args) {
        int port = Integer.parseInt(System.getenv().getOrDefault("GRPC_SERVER_PORT", "8089"));
        var channel = ManagedChannelBuilder
                .forAddress("localhost", port)
                .usePlaintext().build();

        var asyncStub = ConversationServiceGrpc.newBlockingStub(channel);

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);


        Runnable task = () -> {
            var message = Simple.InboundMessage.newBuilder()
                    .setMessage("Hello from Java!")
                    .build();

            var response = asyncStub.handleMessage(message);
            log.info("response: {}", response);
        };

        // Run once after a 3-second delay
        scheduler.schedule(task, 3, TimeUnit.SECONDS);

        // Run repeatedly every 5 seconds, starting after 1 second
        scheduler.scheduleAtFixedRate(task, 1, 5, TimeUnit.SECONDS);
    }

}