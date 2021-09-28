import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

val logbackVersion: String by project
val awsSDKVersion: String by project
val kotlinxSerializationVersion: String by project

plugins {
    kotlin("jvm") version "1.5.31"
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
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:$kotlinxSerializationVersion")
    testImplementation("org.jetbrains.kotlin:kotlin-test-testng:1.5.21")
}

tasks.test {
    useTestNG()
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
                username = ""
                password = ""
            }
        }
    }
}