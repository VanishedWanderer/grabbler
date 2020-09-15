package at.vanishedwanderer.grabbler

import org.intellij.lang.annotations.Language
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import kotlin.properties.Delegates

open class Grabbler<T> internal constructor(
        val statement: PreparedStatement,
        val mapper: ResultSet.() -> T
) {

    /**
     * Run the query and return contents as a List
     */
    open fun execute(): List<T> {
        return statement.executeQuery().asSequence(mapper).toList()
    }

    /**
     * If the PreparedStatements has placeholders, a private API for the query can be defined.
     * @param parameters
     *  A lambda which may return an object (an anonymous object for example) with the fields corresponding to the
     *  placeholders in the Statement.
     * @param parameterApplicator
     *  A lambda which applies the fields of the object given in parameters to the PreparedStatement using set[Type](index, value)
     * @return
     *  A [ParametrizedGrabbler] which contains the parameters applied here
     */
    fun <U> parametrize(parameters: () -> U, parameterApplicator: U.(PreparedStatement) -> Unit): ParametrizedGrabbler<T, U> {
        return ParametrizedGrabbler(this, parameters, parameterApplicator)
    }
}

/**
 * A subtype of Grabbler, created by the [Grabbler.parametrize] method.
 */
class ParametrizedGrabbler<T, U> internal constructor(
    grabbler: Grabbler<T>,
    val parameters: () -> U,
    val parameterApplicator: U.(PreparedStatement) -> Unit
): Grabbler<T>(grabbler.statement, grabbler.mapper) {

    /**
     * Cannot be used in ParametrizedGrabbler, use the overloaded function instead.
     */
    override fun execute(): List<T> {
        throw IllegalStateException("Parametrized Grabbler has to be executed with parameters.")
    }

    /**
     * Execute this query but populate the parameters object first.
     * @param populate
     *   A lambda working on the parameters object, specified by the [Grabbler.parametrize] method.
     *   Set the member variables of this field.
     */
    fun execute(populate: U.() -> Unit): List<T> {
        val params = parameters()
        params.populate()
        statement.clearParameters()
        params.parameterApplicator(statement)
        return super.execute()
    }
}

class ByIdGrabblerParameters<IdType: Number>{
    var id: IdType? = null
}

/**
 * A way to generalize the return values of a query
 * @param queryPrefix
 *   The first part of the SQL-Query. Usually "select ... from ...". This will be extended by the individual grabblers
 *   later.
 * @param mapper
 *   A lambda, mapping the expected [ResultSet] of the [PreparedStatement] to a class. As the mapping from [ResultSet]
 *   to a class requires knowledge of the exact position of the fields, this must be defined in the here, as
 *   one can rely on the staticness of the [queryPrefix]
 *
 */
class GrabblerConfiguration<T>(
        @Language("PostgreSQL")
        val queryPrefix: String,
        val mapper: ResultSet.() -> T
){
    fun createGrabbler(connection: Connection, querySuffix: String): Grabbler<T> {
        return connection.grabbler("$queryPrefix $querySuffix", mapper)
    }

    fun <IdType: Number> createByIdGrabbler(connection: Connection, idFieldName: String): ParametrizedGrabbler<T, ByIdGrabblerParameters<IdType>> {
        return createGrabbler(connection, "where $idFieldName = ?").parametrize({
            ByIdGrabblerParameters()
        }) {
            it.setObject(1, id)
        }
    }
}

/**
 * Create a grabbler using this [Connection]. If you have to create multiple [Grabbler]s which return similar
 * columns, think of using a [GrabblerConfiguration] instead.
 */
fun <T> Connection.grabbler(@Language("PostgreSQL") statement: String, mapper: ResultSet.() -> T): Grabbler<T> {
    println("Preparing Statement: $statement")
    val stmt = this.prepareStatement(statement)
    return Grabbler(stmt, mapper)
}

