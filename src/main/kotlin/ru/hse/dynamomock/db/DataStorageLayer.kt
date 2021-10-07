package ru.hse.dynamomock.db

import ru.hse.dynamomock.model.TableMetadata
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest

interface DataStorageLayer {
    fun createTable(request: CreateTableRequest): TableMetadata

    // TODO support other queries
}