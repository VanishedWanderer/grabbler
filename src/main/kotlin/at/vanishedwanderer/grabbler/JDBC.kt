package at.vanishedwanderer.grabbler

import java.sql.PreparedStatement
import java.sql.ResultSet

/**
 * Takes a list of lists as argument, which will then be executed as a batch.
 * The order in the inner list corresponds to the order of placeholders in the prepared statement.
 */
fun PreparedStatement.executeBatchWith(values: List<List<Any>>): PreparedStatement {
    values.forEach {
        it.forEachIndexed { index, value ->
            this.setObject(index+1, value)
        }
        this.addBatch()
    }
    this.executeBatch()
    return this
}

/**
 * Returns the rows of this ResultSet in a Sequence, mapped to a class by builder
 */
fun <T> ResultSet.asSequence(builder: ResultSet.() -> T): Sequence<T> {
    return sequence {
        while(this@asSequence.next()){
            yield(builder(this@asSequence))
        }
    }
}

/**
 * Useful for retrieving the list of generated keys.
 * <code>
 * val stmt = dataSource.connection.prepareStatement("", Statement.RETURN_GENERATED_KEYS)
 * stmt.generatedKeys.asIdList()
 * </code>
 */
fun <IdType: Number> ResultSet.asIdList(): List<IdType> = this.asSequence { getObject(1) as IdType }.toList()