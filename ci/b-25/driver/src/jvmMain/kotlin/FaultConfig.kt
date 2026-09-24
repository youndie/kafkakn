// The Java client times out a request on the client after `request.timeout.ms`.
actual fun faultConfig(): Map<String, String> = mapOf("request.timeout.ms" to "2000")
