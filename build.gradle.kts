import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

val logbackVersion: String by project
val awsSDKVersion: String by project
val h2dbVersion: String by project
val exposedVersion: String by project
val kotlinCsvVersion: String by project
val kotlinxSerializationVersion: String by project


plugins {
    kotlin("jvm") version "1.6.21"
    kotlin("plugin.serialization") version "1.5.30"
    `maven-publish`
}

group = "ru.hse.dynamo-db-mock"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation("software.amazon.awssdk:dynamodb:$awsSDKVersion")
    implementation("ch.qos.logback:logback-classic:$logbackVersion")
    implementation("com.h2database:h2:$h2dbVersion")
    implementation("org.jetbrains.exposed:exposed-core:$exposedVersion")
    implementation("org.jetbrains.exposed:exposed-dao:$exposedVersion")
    implementation("org.jetbrains.exposed:exposed-jdbc:$exposedVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:$kotlinxSerializationVersion")
    implementation("com.github.doyaaaaaken:kotlin-csv-jvm:$kotlinCsvVersion")

    testImplementation("org.junit.jupiter:junit-jupiter:5.7.0")
    testImplementation("org.jetbrains.kotlin:kotlin-test-testng:1.5.21")
    testImplementation("com.willowtreeapps.assertk:assertk-jvm:0.25")
    testImplementation("org.testcontainers:testcontainers:1.16.3")
    testImplementation("org.testcontainers:junit-jupiter:1.16.3")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile>() {
    kotlinOptions.jvmTarget = "11"
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = "ru.hse.dynamo-db-mock"
            artifactId = "dynamo-db-mock"
            version = "1.1"
            from(components["java"])
        }
    }
    repositories {
        maven {
            url = uri("https://maven.pkg.github.com/sviridov-alexey/aws-dynamodb-mock")
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                password = System.getenv("GITHUB_TOKEN")
            }
        }
    }
}