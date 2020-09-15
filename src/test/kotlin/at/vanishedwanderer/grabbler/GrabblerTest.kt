package at.vanishedwanderer.grabbler

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.postgresql.util.PSQLException
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Testcontainers
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement

@Testcontainers
class GrabblerTest{


    companion object{
        lateinit var connection: Connection

        @BeforeAll
        @JvmStatic
        fun setupContainer(){
            connection = DriverManager.getConnection("jdbc:tc:postgresql:13:///testdb", "", "")
        }
    }

    @BeforeEach
    fun setupDb(){
        val stmt = connection.createStatement()
        stmt.execute("DROP TABLE IF EXISTS test")
        stmt.execute("CREATE TABLE test(id int primary key, text varchar)")
    }

    @Test
    fun queryTest(){
        val stmt = connection.createStatement()
        stmt.execute("INSERT INTO test(id, text) VALUES (3, 'hi')")
        stmt.execute("INSERT INTO test(id, text) VALUES (4, 'hi')")
        val resultSet = stmt.executeQuery("SELECT * FROM test")
        var count = 0
        while(resultSet.next()) count++;
        assertThat(count, `is`(2))
    }

    @Test
    fun jdbc01_asSequence_works(){
        data class Test(
                val id: Int,
                val text: String
        )

        val stmt = connection.createStatement()
        stmt.execute("INSERT INTO test(id, text) VALUES(1, 'hi')")
        stmt.execute("INSERT INTO test(id, text) VALUES(2, 'ho')")
        stmt.execute("INSERT INTO test(id, text) VALUES(3, 'ha')")
        stmt.execute("INSERT INTO test(id, text) VALUES(4, 'he')")

        val resultSet = stmt.executeQuery("SELECT * FROM test")
        val list = resultSet.asSequence {Test(getInt(1), getString(2))}.toList()

        assertThat(list, hasSize(4))
        assertThat(list, hasItem(Test(1, "hi")))
        assertThat(list, hasItem(Test(2, "ho")))
        assertThat(list, hasItem(Test(3, "ha")))
        assertThat(list, hasItem(Test(4, "he")))

    }

    @Test
    fun jdbc02_executeBatchWith_works(){
        //Arrange
        data class Test(
                val id: Int,
                val text: String
        )

        val data = listOf(
                Test(0, "02a"),
                Test(1, "02b"),
                Test(2, "02c"),
                Test(3, "02d"),
                Test(4, "02e"),
                Test(5, "02f"),
                Test(6, "02g"),
        )

        //Act
        val stmt = connection.prepareStatement("INSERT INTO test(id, text) VALUES(?, ?)")
        stmt.executeBatchWith(data.map { listOf(it.id, it.text) })
        val resultSet = connection.createStatement().executeQuery("SELECT * FROM test ORDER BY id")
        val list = resultSet.asSequence {Test(getInt(1), getString(2))}.toList()

        //Assert
        assertThat(list, `is`(equalTo(data)))
    }

    @Test
    fun jdbc03_asIdList_works(){
        //Arrange
        data class Test(
                val id: Int,
                val text: String
        )

        val data = listOf(
                Test(0, "02a"),
                Test(1, "02b"),
                Test(2, "02c"),
                Test(3, "02d"),
                Test(4, "02e"),
                Test(5, "02f"),
                Test(6, "02g"),
        )

        //Act
        val stmt = connection.prepareStatement("INSERT INTO test(id, text) VALUES(?, ?)", PreparedStatement.RETURN_GENERATED_KEYS)
        stmt.executeBatchWith(data.map { listOf(it.id, it.text) })
        val list = stmt.generatedKeys.asIdList<Int>()

        //Assert
        assertThat(list, `is`(equalTo(data.map { it.id })))
    }

    @Test
    fun grabbler01_createGrabbler_works(){
        //Arrange
        data class Test(
                val id: Int,
                val text: String
        )

        //Act
        val grabbler = connection.grabbler("SELECT id, text from test") {
            Test(getInt(1), getString(2))
        }
        grabbler.execute()

        //Assert: Not thrown exception
    }
    @Test
    fun grabbler01a_createGrabbler_invalidQuery_throws(){
        //Arrange
        data class Test(
                val id: Int,
                val text: String
        )

        //Act
        val grabbler = connection.grabbler("SELECT id, OINK!!, text from test") {
            Test(getInt(1), getString(2))
        }

        assertThrows<PSQLException> {
            grabbler.execute()
        }

        //Assert: Not thrown exception
    }

    @Test
    fun grabbler02_simpleQuery_works(){
        //Arrange
        data class Test(
                val id: Int,
                val text: String
        )
        val data = listOf(
                Test(0, "02a"),
                Test(1, "02b"),
                Test(2, "02c"),
                Test(3, "02d"),
                Test(4, "02e"),
                Test(5, "02f"),
                Test(6, "02g"),
        )
        val grabbler = connection.grabbler("SELECT id, text from test") {
            Test(getInt(1), getString(2))
        }

        //Act
        val stmt = connection.prepareStatement("INSERT INTO test(id, text) VALUES(?, ?)", PreparedStatement.RETURN_GENERATED_KEYS)
        stmt.executeBatchWith(data.map { listOf(it.id, it.text) })
        val result = grabbler.execute()

        //Assert
        assertThat(result, `is`(equalTo(data)))
    }
}
