plugins {
    id("java")
}

group = "se.ifmo"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // Hadoop MapReduce (локальный режим)
    implementation("org.apache.hadoop:hadoop-client:3.3.6")

    // Логирование
    implementation("org.slf4j:slf4j-api:2.0.13")
    runtimeOnly("ch.qos.logback:logback-classic:1.5.6")

    // JUnit 5 для тестов
    testImplementation(platform("org.junit:junit-bom:5.10.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}