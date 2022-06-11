package ru.hse.dynamomock.service

import ru.hse.dynamomock.db.DataStorageLayer
import ru.hse.dynamomock.exception.dynamoException
import ru.hse.dynamomock.exception.dynamoRequires
import ru.hse.dynamomock.model.TableMetadata
import ru.hse.dynamomock.model.query.SelectRequest
import ru.hse.dynamomock.model.query.SelectResponse
import ru.hse.dynamomock.model.query.toSelectRequest
import ru.hse.dynamomock.model.sortKey
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructList
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructMap
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.QueryResponse
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.ScanResponse
import software.amazon.awssdk.services.dynamodb.model.Select

class SelectService(
    private val storage: DataStorageLayer,
    private val tablesMetadata: MutableMap<String, TableMetadata>
) {

    fun query(request: QueryRequest): QueryResponse = request.toSelectRequest().select().toQueryResponse()

    fun scan(request: ScanRequest): ScanResponse = request.toSelectRequest().select().toScanResponse()

    // TODO take into account that it's impossible to use expression-like and not-expression-like at the same query
    private fun SelectRequest.select(): SelectResponse {
        dynamoRequires(limit == null || limit > 0) {
            "Limit must be >= 1."
        }
        dynamoRequires(consistentRead != false) {
            "Non-consistent read is not supported in query yet."
        }

        dynamoRequires(tableName in tablesMetadata) {
            "Cannot query from non-existent table $tableName."
        }

        val table = tablesMetadata.getValue(tableName)
        val index = indexName?.let { table.localSecondaryIndex(it) }
        val (keyConditions, usedKeyExpr) = retrieveKeyConditions()
        keyConditions?.also { keyConditions ->
            keyConditions[table.partitionKey].let {
                dynamoRequires(it != null && it.comparisonOperator() == ComparisonOperator.EQ) {
                    "Partition key must use '=' operator."
                }
            }
            dynamoRequires(keyConditions.size <= 2) {
                "Length of key conditions must be 1 or 2."
            }
            if (index == null) {
                dynamoRequires(keyConditions.size == 1 || table.sortKey in keyConditions) {
                    "Only partition and sort keys can be used in key conditions."
                }
            } else {
                dynamoRequires(keyConditions.size == 1 || index.sortKey in keyConditions) {
                    "Only partition key and index sort key can be used in key conditions."
                }
            }
        }

        val items = storage.query(tableName, keyConditions).let {
            if (scanIndexForward) it else it.reversed()
        }.map { item ->
            item.associate { it.name to it.type.toAttributeValue() }
        }.let { items ->
            exclusiveStartKey?.let { exclusiveStartKey ->
                val sortKey = exclusiveStartKey[table.sortKey]
                items.asSequence()
                    .dropWhile { it[table.sortKey] != sortKey }
                    .drop(1)
                    .toList()
            } ?: items
        }.let { items ->
            limit?.let { items.take(it) } ?: items
        }

        val (filterExpression, usedFilterExpr) = retrieveFilterExpression()
        // TODO: fix message
        if ((usedFilterExpr + usedKeyExpr).size != expressionAttributeValues.size) {
            throw dynamoException("Value provided in ExpressionAttributeValues unused in expressions:")
        }
        val filteredItems = filterExpression?.let {
            items.filter {
                val withoutKeys = it.toMutableMap().apply {
                    remove(table.partitionKey)
                    remove(table.sortKey)
                }
                filterExpression.evaluate(withoutKeys)
            }
        } ?: items

        val responseBuilder = SelectResponse.Builder().apply {
            count = filteredItems.size
            scannedCount = items.size
        }

        if (items.size == limit) {
            responseBuilder.lastEvaluatedKey = items.last()
        } else {
            responseBuilder.lastEvaluatedKey = DefaultSdkAutoConstructMap.getInstance()
        }

        val transformer = retrieveAttributesTransformer(table)
        if (select == Select.COUNT) {
            responseBuilder.items = DefaultSdkAutoConstructList.getInstance()
        } else {
            responseBuilder.items = filteredItems.map(transformer)
        }
        return responseBuilder.build()
    }
}