package ru.hse.dynamomock.exception

import software.amazon.awssdk.awscore.exception.AwsServiceException
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException

inline fun dynamoRequires(value: Boolean, lazyMessage: () -> Any = { "" }) {
    if (!value) {
        throw dynamoException(lazyMessage().toString())
    }
}

fun dynamoException(message: String): AwsServiceException = DynamoDbException.builder().message(message).build()
