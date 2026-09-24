// NOT `request.timeout.ms`: in librdkafka that one "is only enforced by the broker", which is frozen.
// The client gives up on a request after `socket.timeout.ms` - sixty seconds by default, longer than
// any pause this fixture makes, so without this line the native arm would never retry anything.
actual fun faultConfig(): Map<String, String> = mapOf("socket.timeout.ms" to "2000")
