import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import kotlinx.coroutines.runBlocking

/** GETs each URL given and prints its status and the start of its body, or what failed. */
fun main(args: Array<String>) =
    runBlocking {
        val client = HttpClient(CIO)
        try {
            for (url in args) {
                val said =
                    try {
                        val response = client.get(url)
                        "${response.status.value} ${response.bodyAsText().take(60).replace('\n', ' ')}"
                    } catch (failure: Exception) {
                        "FAILED ${failure::class.simpleName}: ${failure.message}"
                    }
                println("$url -> $said")
            }
        } finally {
            client.close()
        }
    }
