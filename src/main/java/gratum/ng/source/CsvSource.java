package gratum.ng.source;

import gratum.csv.CSVFile;
import gratum.csv.CSVReader;
import gratum.ng.etl.Pipeline;

import java.io.*;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;


/**
 * Reads a delimited separated text file into a series of rows.  It's most common format is the comma separated values (csv),
 * but this supports any value separated format (i.e. tab, colon, comma, pipe, etc).  Here is a simple example:
 *
 * <pre>
 *     csv( "/resources/titanic.csv" ).filter([ Embarked: "Q"]).go()
 * </pre>
 *
 * Example changing the delimiter:
 *
 * <pre>
 *     csv("/resources/pipe_separated_example.csv", "|")
 *          .filter([ someProperty: "someValue" ])
 *          .go()
 * </pre>
 *
 * Example header-less file:
 *
 * <pre>
 *     csv("/resources/headerless.csv", "|", ["date", "status", "client-ip", "server-name", "url", "length", "thread", "user-agent", "referer"])
 *          .filter { Map row -> row["server-name}.contains("myhostname") }
 *          .go()
 * </pre>
 *
 * From external InputStream
 *
 * <pre>
 *     csv( "External InputStream", stream, "|" ).filter( [ someColumn: "someValue" ] ).go()
 * </pre>
 */
public class CsvSource extends AbstractSource {

    CSVFile csvFile;

    Consumer<List<String>> headerClosure = null;

    protected CsvSource(File file, String separator, List<String> headers) {
        this.name = file.getName();
        csvFile = new CSVFile( file, separator );
        if( headers != null ) csvFile.setColumnHeaders( headers );
    }

    protected CsvSource(String name, Reader reader, String separator, List<String> headers) {
        this.name = name;
        csvFile = new CSVFile( reader, separator );
        if( headers != null ) csvFile.setColumnHeaders( headers );
    }

    public static CsvSource of(File file) {
        return of( file, ",", null );
    }
    
    public static CsvSource of(File file, String separator ) {
        return of( file, separator, null );
    }

    public static CsvSource of(File file, String separator, List<String> headers) {
        return new CsvSource( file, separator, headers );
    }

    public static CsvSource of(String filename) {
        return of( new File( filename ), "," );
    }

    public static CsvSource of(String filename, String separator) {
        return of( new File( filename ), separator );
    }

    public static CsvSource of(String filename, String separator, List<String> headers) {
        return of( new File( filename ), separator, headers );
    }

    public static Pipeline csv( File file ) {
        return csv( file, "," );
    }

    public static Pipeline csv( File file, String separator ) {
        return csv( file, separator, null );
    }

    public static Pipeline csv( File file, String separator, List<String> headers ) {
        return new CsvSource( file, separator, headers ).into();
    }

    public static Pipeline csv( String filename ) {
        return csv( new File(filename), ",", null );
    }

    public static Pipeline csv( String filename, String separator ) {
        return csv( new File(filename), separator, null );
    }

    public static Pipeline csv( String filename, String separator, List<String> headers ) {
        return csv( new File(filename), separator, headers );
    }

    public static Pipeline csv(String name, InputStream stream, String separator, List<String> headers) {
        return new CsvSource( name, new InputStreamReader(stream), separator, headers ).into();
    }

    public CsvSource header( Consumer<List<String>> headerClosure ) {
        this.headerClosure = headerClosure;
        return this;
    }

    @Override
    public void start(Pipeline pipeline) {
        CSVReader csvReader = new CSVReader() {
            int line = 1;

            @Override
            public void processHeaders(List<String> header) {
                if( headerClosure != null ) {
                    headerClosure.accept( header );
                }
            }

            @Override
            public boolean processRow(List<String> header, List<String> row) {
                Map<String,Object> obj = new LinkedHashMap<>();
                for( int i = 0; i < row.size(); i++ ) {
                    obj.put(header.get(i), row.get(i));
                }

                if( header.size() > row.size() ) {
                    for( int j = row.size(); j < header.size(); j++ ) {
                        obj.put( header.get(j), null);
                    }
                }

                return pipeline.process( obj, line++ );
            }

            @Override
            public void afterProcessing() {
            }
        };
        try {
            csvFile.parse(csvReader);
        } catch (IOException e) {
            throw new RuntimeException( e.getMessage(), e );
        }
    }
}
