package ru.hse.dynamomock.db

import ru.hse.dynamomock.db.util.SqlQuerier
import ru.hse.dynamomock.model.TableMetadata
import software.amazon.awssdk.services.dynamodb.model.*
import java.sql.Connection
import java.sql.DriverManager

@Suppress("unused")
class HSQLDBStorage(
    dbname: String,
    username: String = "sa",
    password: String = ""
): DataStorageLayer, AutoCloseable {
    private val connection: Connection = connect(dbname, username, password)

    override fun close() = connection.close()

    override fun createTable(request: CreateTableRequest): TableMetadata {
        val tableMetadata = TableMetadata(
            request.tableName(),
            request.attributeDefinitions(),
            request.keySchema(),
            TableStatus.ACTIVE
        )
        val createTableQuery = SqlQuerier.createTableQuery(tableMetadata)
        connection.prepareStatement(createTableQuery).use { it.execute() }

        return tableMetadata
    }

    companion object {
        init {
            Class.forName("org.hsqldb.jdbcDriver")
        }

        private fun connect(dbname: String, username: String, password: String) =
            DriverManager.getConnection("jdbc:hsqldb:mem:$dbname", username, password)
    }
}