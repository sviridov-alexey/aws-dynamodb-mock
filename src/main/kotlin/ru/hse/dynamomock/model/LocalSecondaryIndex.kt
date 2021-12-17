package ru.hse.dynamomock.model

import ru.hse.dynamomock.exception.dynamoException
import software.amazon.awssdk.services.dynamodb.model.KeyType
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndex

val LocalSecondaryIndex.sortKey
    get(): String = keySchema().firstOrNull { it.keyType() == KeyType.RANGE }?.attributeName()
        ?: throw dynamoException("Local Secondary Index must contains sort key.")

