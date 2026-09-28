plugins {
    application
}

java {
    toolchain { languageVersion.set(JavaLanguageVersion.of(21)) }
}

dependencies {
    implementation("io.confluent:kafka-json-schema-serializer:8.3.2")
    implementation("io.confluent:kafka-protobuf-serializer:8.3.2")
    implementation("org.apache.kafka:kafka-clients:4.3.1")
    runtimeOnly("org.slf4j:slf4j-nop:2.0.17")
}

application {
    mainClass.set("Oracle")
}
