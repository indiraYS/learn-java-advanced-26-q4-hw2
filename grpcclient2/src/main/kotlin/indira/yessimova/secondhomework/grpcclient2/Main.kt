import indira.yessimova.secondhomework.ConversationServiceGrpcKt
import indira.yessimova.secondhomework.Simple
import io.github.oshai.kotlinlogging.KotlinLogging
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

private val log = KotlinLogging.logger {}

fun main() = runBlocking {

    val port = System.getenv("GRPC_SERVER_PORT")?.toInt() ?: 8089
    val channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build()
    val stub = ConversationServiceGrpcKt.ConversationServiceCoroutineStub(channel)


    var job = scheduleJob(3.seconds) {
        var response = stub.handleMessage(Simple.InboundMessage.newBuilder()
            .setMessage("Hello from Kotlin!")
            .build())

        log.info { "response: ${response.message}" };
    }

    delay(15.seconds) // delay execution to see the logs
    job.join()
}


fun CoroutineScope.scheduleJob(
    period: Duration,
    initialDelay: Duration = 0.seconds,
    action: suspend () -> Unit
): Job = launch(Dispatchers.Default) {
    delay(initialDelay) // Wait for the first time run
    while (isActive) {
        try {
            action() // perform action
        } catch (e: Exception) {
            println("Error during job execution: ${e.message}")
        }
        delay(period) // pause
    }
}
