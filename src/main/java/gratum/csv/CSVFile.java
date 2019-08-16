package gratum.csv;

import gratum.util.Utilities;
import org.apache.commons.io.input.BOMInputStream;

import java.io.*;
import java.text.MessageFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CSVFile {

    //    private static final String CSV_PATTERN = "(\"([^\"]*)\"|[^{0}]*){0}?";
    private static final String CSV_PATTERN = "(\"(([^\\\"]??(\\\\\\\")?)*)\"|[^{0}]*){0}?";

    private File file;
    private Reader reader;
    private String separator;
    private boolean header = true;
    private PrintWriter writer;
    private String lastLine;
    private Pattern linePattern;

    private int rows = 0;
    private String[] columnHeaders;
    private HashSet<String> rowHashes = new HashSet<String>();
    private boolean allowDuplicateRows=true;

    public CSVFile(String filename, String separator) {
        this( new File(filename), separator );
    }

    public CSVFile(File file, String separator) {
        this.file = file;
        this.separator = separator;

        linePattern = Pattern.compile(MessageFormat.format(CSV_PATTERN, Pattern.quote(separator)));
    }

    public CSVFile(Reader reader, String separator) {
        this.reader = reader;
        this.separator = separator;
        linePattern = Pattern.compile(MessageFormat.format(CSV_PATTERN, Pattern.quote(separator)));
    }

    public CSVFile(PrintWriter out, String separator) {
        this.writer = out;
        this.separator = separator;

        linePattern = Pattern.compile(MessageFormat.format(CSV_PATTERN, Pattern.quote(separator)));
    }

    public void setAllowDuplicateRows(boolean allowDuplicateRows) {
        this.allowDuplicateRows = allowDuplicateRows;
    }

    public boolean getAllowDuplicateRows() {
        return this.allowDuplicateRows;
    }

    public int parse( CSVReader callback ) throws IOException {
        if( file != null ) {
            BOMInputStream bom = new BOMInputStream(new FileInputStream(file));
            Reader reader = bom.hasBOM() ? new InputStreamReader(bom, bom.getBOMCharsetName()) : new InputStreamReader(bom, "UTF-8");
            return parse(reader, callback);
        } else {
            return parse(reader,callback);
        }
    }

    protected int parse(Reader reader, CSVReader callback) throws IOException {
        LineNumberReader lineNumberReader = new LineNumberReader(reader);
        int lines = 1;
        List<String> headers = null;
        if( header ) {
            try {
                headers = readNext(lineNumberReader);
                callback.processHeaders( headers );
                lines++;
            } catch( Exception ex ) {
                throw new IOException( "Could not process header " + lines + ": " + lastLine, ex );
            }
        }

        try {
            List<String> row = null;
            while( (row = readNext(lineNumberReader)) != null ) {
                boolean stop = callback.processRow( headers, row );
                if( stop ) {
                    return lines;
                }
                lines++;
            }
            return lines;
        } catch( RuntimeException ex ) {
            throw new RuntimeException( "Could not parse line " + lines + ": " +  lastLine, ex );
        } catch( Exception ex ) {
            throw new IOException( "Could not process line " + lines + ": " + lastLine, ex );
        } finally {
            lineNumberReader.close();
            callback.afterProcessing();
        }
    }

    private List<String> readNext( LineNumberReader reader ) throws IOException {
        do {
            lastLine = reader.readLine();
            if( lastLine == null ) return null;
        } while( lastLine.length() == 0 );

        List<String> line = new ArrayList<String>();
        Matcher matcher = linePattern.matcher(lastLine);
        int current = 0;
        while( current < lastLine.length() && matcher.find(current) ) {
            if( matcher.group(2) !=null ) {
                line.add( unescape(matcher.group(2)) );
            } else if( matcher.group(1) != null ) {
                line.add( unescape(matcher.group(1)) );
            } else {
                line.add( unescape(matcher.group(0)) );
            }
            current = matcher.end();
        }

        return line;
    }

    private String unescape( String input ) {
        return input.replace("\\n", "\n").replace("\\\"", "\"");
    }

    public void write( Map row, String[] columnHeaders ) throws IOException {
        this.columnHeaders = columnHeaders;
        String[] rowArray = new String[columnHeaders.length];
        int i = 0;
        for (String columnHeader : columnHeaders) {
            rowArray[i++] = row.get(columnHeader) == null ? "" : row.get(columnHeader).toString();
        }
        write(rowArray);
    }

    public void write( LinkedHashMap row ) throws IOException {
        if (rows == 0) {
            columnHeaders = new String[row.keySet().size()];
            int i = 0;
            for (Object headerKey : row.keySet()) {
                columnHeaders[i++] = headerKey.toString();
            }
            write(columnHeaders);
        }
        String[] rowArray = new String[columnHeaders.length];
        int i = 0;
        for (String columnHeader : columnHeaders) {
            if (row.get(columnHeader) == null) {
                rowArray[i++] = "";
            } else {
                rowArray[i++] = row.get(columnHeader).toString();
            }
        }
        write(rowArray);
    }
    public void write( Object... row ) throws IOException {
        boolean addRow = true;
        if(!allowDuplicateRows) {
            String rowString = "";
            for(Object rowValue : row) {
                rowString += "_"+rowValue.toString();
            }
            String rowHash = Utilities.MD5(rowString);
            if(!rowHashes.contains(rowHash)) {
                rowHashes.add(rowHash);
            }else {
                addRow = false;
            }
        }

        if(addRow) {
            if( writer == null ) {
                writer = new PrintWriter( new FileWriter(file) );
            }
            StringBuilder buffer = new StringBuilder();
            for( int i = 0; i < row.length; i++ ) {
                if( i > 0 ) {
                    buffer.append(separator);
                }
                if( row[i] != null ) {
                    buffer.append( escape( format( row[i] ) ) );
                }else {
                    buffer.append( "\"\"" );
                }
            }
            writer.println( buffer.toString() );
            rows++;
        }
    }

    private String format(Object o) {
        return o.toString();
    }

    private CharSequence escape(String source) {
        return "\"" + source.replace("\"", "\\\"") + "\"";
    }

    public void flush() {
        if( writer != null ) {
            writer.flush();
        }
    }
    public void close() {
        flush();
        if( writer != null ) {
            writer.close();
        }
    }

    public int getRows() {
        return rows;
    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public String getSeparator() {
        return separator;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }

    public boolean isHeader() {
        return header;
    }

    public void setHeader(boolean header) {
        this.header = header;
    }

    public void setWriter( Writer writer ) {
        this.writer = new PrintWriter( writer );
    }
}
