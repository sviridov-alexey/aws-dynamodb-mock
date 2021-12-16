package ru.hse.dynamomock.model.query

import software.amazon.awssdk.core.util.DefaultSdkAutoConstructList
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructMap
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity
import software.amazon.awssdk.services.dynamodb.model.QueryResponse
import software.amazon.awssdk.services.dynamodb.model.ScanResponse

data class SelectResponse(
    val items: List<Map<String, AttributeValue>>,
    val count: Int,
    val scannedCount: Int,
    val lastEvaluatedKey: Map<String, AttributeValue>?,
    val consumedCapacity: ConsumedCapacity?
) {
    class Builder(
        var items: List<Map<String, AttributeValue>> = emptyList(),
        var count: Int = 0,
        var scannedCount: Int = 0,
        var lastEvaluatedKey: Map<String, AttributeValue>? = null,
        var consumedCapacity: ConsumedCapacity? = null
    ) {
        fun build() = SelectResponse(items, count, scannedCount, lastEvaluatedKey, consumedCapacity)
    }

    fun toQueryResponse(): QueryResponse = QueryResponse.builder()
        .items(items)
        .count(count)
        .scannedCount(scannedCount)
        .lastEvaluatedKey(lastEvaluatedKey)
        .consumedCapacity(consumedCapacity)
        .build()

    fun toScanResponse(): ScanResponse = ScanResponse.builder()
        .items(items)
        .count(count)
        .scannedCount(scannedCount)
        .lastEvaluatedKey(lastEvaluatedKey)
        .consumedCapacity(consumedCapacity)
        .build()
}

fun QueryResponse.toSelectResponse() = SelectResponse(
    items = items().takeIf { hasItems() } ?: DefaultSdkAutoConstructList.getInstance(),
    count = count(),
    scannedCount = scannedCount(),
    lastEvaluatedKey = lastEvaluatedKey().takeIf { hasLastEvaluatedKey() } ?: DefaultSdkAutoConstructMap.getInstance(),
    consumedCapacity = consumedCapacity()
)

fun ScanResponse.toSelectResponse() = SelectResponse(
    items = items().takeIf { hasItems() } ?: DefaultSdkAutoConstructList.getInstance(),
    count = count(),
    scannedCount = scannedCount(),
    lastEvaluatedKey = lastEvaluatedKey().takeIf { hasLastEvaluatedKey() } ?: DefaultSdkAutoConstructMap.getInstance(),
    consumedCapacity = consumedCapacity()
)
