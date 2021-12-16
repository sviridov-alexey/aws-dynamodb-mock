package ru.hse.dynamomock.model.query

import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity
import software.amazon.awssdk.services.dynamodb.model.QueryResponse
import software.amazon.awssdk.services.dynamodb.model.ScanResponse

data class SelectResponseBuilder(
    var items: List<Map<String, AttributeValue>> = emptyList(),
    var count: Int = 0,
    var scannedCount: Int = 0,
    var lastEvaluatedKey: Map<String, AttributeValue>? = null,
    var consumedCapacity: ConsumedCapacity? = null
) {
    fun toQueryResponse() = QueryResponse.builder()
        .items(items)
        .count(count)
        .scannedCount(scannedCount)
        .lastEvaluatedKey(lastEvaluatedKey)
        .consumedCapacity(consumedCapacity)
        .build()

    fun toScanResponse() = ScanResponse.builder()
        .items(items)
        .count(count)
        .scannedCount(scannedCount)
        .lastEvaluatedKey(lastEvaluatedKey)
        .consumedCapacity(consumedCapacity)
        .build()
}
