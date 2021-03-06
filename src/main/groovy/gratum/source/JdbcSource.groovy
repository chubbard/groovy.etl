package gratum.source

import gratum.etl.Pipeline
import groovy.sql.GroovyResultSet
import groovy.sql.Sql

import java.sql.ResultSetMetaData

/**
 * A source that uses a database query for the source of the rows it feeds through the pipeline.
 * This source can actually be re-used for different database queries.  For example:
 *
 * <pre>
 *     database( sql )
 *      .query("select * from People where age >= ${age}")
 *      .into("Age")
 *      .go()
 * </pre>
 */
class JdbcSource extends AbstractSource {

    Sql db
    GString query

    JdbcSource(Sql db) {
        this.name = "jdbc"
        this.db = db
    }

    JdbcSource(String url, String username, String password) {
        this.name = url
        db = Sql.newInstance(url, username, password)
    }

    static JdbcSource database( Sql sql ) {
        return new JdbcSource(sql)
    }

    static JdbcSource database(String url, String username, String password) {
        return new JdbcSource(url, username, password)
    }

    JdbcSource query( GString query ) {
        this.query = query
        return this
    }

    @Override
    void start(Pipeline pipeline) {
        List<String> columns = []
        int line = 1
        db.eachRow( query, { ResultSetMetaData md ->
            for( int i = 1; i <= md.columnCount; i++ ) {
                columns << md.getColumnName(i)
            }
        } ) { GroovyResultSet row ->
            Map result = [:]
            columns.eachWithIndex { String col, int index ->
                result[col] = row[index]
            }
            pipeline.process( result, line )
        }
    }
}
