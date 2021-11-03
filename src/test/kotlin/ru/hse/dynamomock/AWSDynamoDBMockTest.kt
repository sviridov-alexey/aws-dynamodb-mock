package ru.hse.dynamomock

import org.junit.jupiter.api.BeforeEach
import kotlin.properties.Delegates

internal open class AWSDynamoDBMockTest {
    protected var mock: AWSDynamoDBMock by Delegates.notNull()
        private set

    @BeforeEach
    fun init() {
        mock = AWSDynamoDBMock()
    }
}
