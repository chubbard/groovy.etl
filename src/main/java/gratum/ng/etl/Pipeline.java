package gratum.ng.etl;

import gratum.csv.CSVFile;
import gratum.csv.HaltPipelineException;
import gratum.ng.source.ChainedSource;
import gratum.ng.source.Source;
import gratum.ng.source.AbstractSource;
import groovy.lang.GString;
import org.apache.commons.math3.util.Pair;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class Pipeline {

    private static final String REJECTED_KEY = "__reject__";

    private final LoadStatistic statistic;
    private Source src;
    private final List<Step<Map<String,Object>,Map<String,Object>>> processChain = new ArrayList<>();
    private final List<Runnable> doneChain = new ArrayList<>();
    private Pipeline rejections;
    private boolean complete = false;

    public Pipeline(String name) {
        this.statistic = new LoadStatistic(name);
    }

    public String getName() {
        return this.statistic.getName();
    }

    public Pipeline addStep(String name, Function<Map<String,Object>,Map<String,Object>> step ) {
        processChain.add( new Step<>( name, step ) );
        return this;
    }

    public Pipeline addStep(GString name, Function<Map<String,Object>,Map<String,Object>> step) {
        return this.addStep( name.toString(), step );
    }

    public Pipeline after(Runnable step ) {
        doneChain.add( step );
        return this;
    }

    public Pipeline onRejection(Function<Pipeline,Void> branch ) {
        if( rejections == null ) rejections = new Pipeline("Rejections(" + getName() + ")");
        branch.apply( rejections );
        after(() -> {
            rejections.doneChain.forEach(Runnable::run);
        });
        return this;
    }

    public Pipeline renameFields(Map<String,String> fields) {
        String description = fields.entrySet().stream().map((e) -> e.getKey() + " -> " + e.getValue() ).collect(Collectors.joining(",", "rename(", ")"));
        return addStep(description, (Map<String,Object> row) -> {
            for( String src : fields.keySet() ) {
                String dest = fields.get( src );
                row.put(dest, row.remove( src ));
            }
            return row;
        });
    }

    public Pipeline filter( Map<String,Object> columns ) {
        return filter( Condition.Builder.from( columns ) );
    }

    public Pipeline filter( Condition condition ) {
        return addStep( "filter " + condition, (Map<String,Object> row) -> {
            if( condition.matches(row) ) {
                return row;
            } else {
                return reject(row,"Row did not match the filter " + condition, RejectionCategory.IGNORE_ROW );
            }
        });
    }

    public Pipeline trim() {
        return addStep("trim()",( Map<String,Object> row ) -> {
            row.forEach( (String key, Object value) -> {
                row.put(key, value.toString().trim() );
            });
            return row;
        });
    }

    public Pipeline branch( Consumer<Pipeline> split ) {
        return branch("No Name Branch", split);
    }

    public Pipeline branch( String name, Consumer<Pipeline> split ) {
        Pipeline branch = new Pipeline( name );
        split.accept(branch);

        return addStep("branch()", (Map<String,Object> row) -> {
            branch.process( new LinkedHashMap<>(row), 0 );
            return row;
        });
    }

    public Pipeline branch(Condition selection, Function<Pipeline,Pipeline> split) {
        Pipeline branch = new Pipeline( "Branch( " + selection + ")" );
        Pipeline tail = split.apply(branch);

        addStep( "branch(" + selection.toString() + ")", ( Map<String,Object> row ) -> {
            if( selection.matches( row )) {
                branch.process( new LinkedHashMap<>(row), 0 );
            }
            return row;
        });

        after(tail::runDoneChain);
        return this;
    }

    public Pipeline branch(Map<String,Object> condition, Function<Pipeline,Pipeline> split) {
        return branch( Condition.Builder.from( condition ), split );
    }

    public Pipeline groupBy( String... columns ) {
        // todo this is not thread friendly
        Map<String,Object> cache = new HashMap<>();
        addStep(renderName("groupBy", columns), (Map<String,Object> row) -> {
            Map<String, Object> current = cache;
            for (int i = 0; i < columns.length; i++) {
                String col = columns[i];
                String key = row.get(col).toString();
                if (!current.containsKey(key)) {
                    if (i + 1 < columns.length) {
                        current.put(key, new HashMap<>());
                        current = (Map<String,Object>) current.get(key);
                    } else {
                        current.put(key, new ArrayList<>());
                    }
                } else if (i + 1 < columns.length) {
                    current = (Map<String,Object>) current.get(key);
                }
            }
            List<Map<String,Object>> list = (ArrayList<Map<String,Object>>)current.get(row.get(columns[columns.length - 1]));
            list.add( row );
            return row;
        });

        Pipeline parent = this;
        Pipeline other = new Pipeline( getName() );
        other.setSrc( new AbstractSource() {
            @Override
            public void start(Pipeline pipeline) {
                parent.start(); // first start our parent pipeline
                pipeline.process( cache, 1 );
            }
        });
        other.copyStatistics( this );
        return other;
    }

    public Pipeline sort(String... columns) {
        // todo sort externally

        Comparator<Map<String,Object>> comparator = new Comparator<Map<String,Object>>() {
            @Override
            public int compare(Map<String,Object> o1, Map<String,Object> o2) {
                for( String key : columns ) {
                    Comparable c1 = (Comparable) o1.get(key);
                    Comparable c2 = (Comparable) o2.get(key);
                    int value = c1.compareTo( c2 );
                    if( value != 0 ) return value;
                }
                return 0;
            }
        };

        List<Map<String,Object>> ordered = new ArrayList<>();
        addStep(renderName( "sort", columns ), (Map<String,Object> row) -> {
            //int index = Collections.binarySearch( ordered, row, comparator )
            //ordered.add( Math.abs(index + 1), row )
            ordered.add(row);
            return row;
        });

        Pipeline next = new Pipeline(getName());
        next.src = new ChainedSource( this );
        after( () -> {
            next.statistic.copy(this.statistic);
            ordered.sort( comparator );
            ((ChainedSource)next.src).process( ordered );
        });

        return next;
    }

    Pipeline asDouble(String column) {
        return addStep("asDouble(" + column + ")", (Map<String,Object> row) -> {
            String value = (String)row.get(column);
            try {
                if (value != null) row.put(column, Double.parseDouble(value));
                return row;
            } catch( NumberFormatException ex) {
                return reject(row, "Could not parse " + value + " as a Double", RejectionCategory.INVALID_FORMAT);
            }
        });
    }

    public Pipeline asInt(String column) {
        return addStep("asInt(" + column + ")", (Map<String,Object> row) -> {
            String value = (String) row.get(column);
            try {
                if( value != null ) row.put(column, Integer.parseInt(value) );
                return row;
            } catch( NumberFormatException ex ) {
                return reject(row, "Could not parse " + value + " to an integer.", RejectionCategory.INVALID_FORMAT);
            }
        });
    }

    Pipeline asBoolean(String column) {
        addStep("asBoolean(" + column + ")", ( Map<String,Object> row) -> {
            String value = (String)row.get(column);
            if( value != null ) {
                switch( value ) {
                    case "Y":
                    case "y":
                    case "yes":
                    case "YES":
                    case "Yes":
                    case "1":
                    case "T":
                    case "t":
                        row.put(column, true);
                        break;
                    case "n":
                    case "N":
                    case "NO":
                    case "no":
                    case "No":
                    case "0":
                    case "F":
                    case "f":
                    case "null":
                    case "Null":
                    case "NULL":
                        row.put(column, false);
                        break;
                    default:
                        row.put(column, Boolean.parseBoolean(value));
                        break;
                }
            }
            return row;
        });
        return this;
    }

    public Pipeline asDate( String column ) {
        return asDate( column, "yyyy-MM-dd" );
    }

    public Pipeline asDate(String column, String format) {
        // todo this is not thread friendly
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        addStep("asDate(" + column + ", " + format + ")", (Map<String,Object> row) -> {
            String val = (String)row.get(column);
            try {
                if (val != null) row.put(column, dateFormat.parse(val));
                return row;
            } catch( ParseException ex ) {
                return reject( row,row.get(column) + " could not be parsed by format " + format, RejectionCategory.INVALID_FORMAT );
            }
        });
        return this;
    }

    public Pipeline concat(Pipeline src ) {
        Pipeline original = this;
        this.after( () -> {
            AtomicInteger line = new AtomicInteger(1);
            src.addStep("concat(" + src.getName() + ")", (Map<String,Object> row) -> {
                original.process( row, line.getAndIncrement());
                return row;
            }).start(null);
        } );
        return this;
    }

    public Pipeline exchange(Function<Map<String,Object>, Pipeline> closure) {
        Pipeline next = new Pipeline( getName() );
        next.setSrc( new ChainedSource(this) );
        addStep("exchange()", ( Map<String,Object> row ) -> {
            Pipeline pipeline = closure.apply( row );
            pipeline.start( (Map<String,Object> current) -> {
                next.process( current, 0 );
                return current;
            });
            return row;
        } );
        next.copyStatistics( this );
        return next;
    }

    public Pipeline setField(String fieldName, Object value ) {
        return addStep("setField(" + fieldName + ")", ( Map<String,Object> row ) -> {
            row.put(fieldName, value);
            return row;
        });
    }

    public Pipeline addField(String fieldName, Function<Map<String,Object>,Object> fieldValue) {
        return addStep("addField(" + fieldName + ")", ( Map<String,Object> row ) -> {
            Object value = fieldValue.apply(row);
            if( value instanceof Rejection ) {
                row.put(REJECTED_KEY, value);
                return row;
            }
            row.put(fieldName, value);
            return row;
        });
    }

    public Pipeline removeField(String fieldName, Function<Map<String,Object>,Boolean> removeLogic) {
        return addStep( "removeField(" + fieldName + ")", (Map<String,Object> row) -> {
            if (removeLogic == null || removeLogic.apply(row)) {
                row.remove(fieldName);
            }
            return row;
        });
    }

    public Pipeline save( String filename ) {
        return save( filename, "," );
    }

    public Pipeline save( String filename, String separator ) {
        return save( filename, separator, null );
    }

    public Pipeline save( String filename, String separator, List<String> columns ) {
        CSVFile out = new CSVFile( filename, separator );
        if( columns != null ) out.setColumnHeaders( columns );
        addStep("Save to " + out.getFile().getName(), (Map<String,Object> row) -> {
            try {
                out.write( row );
            } catch (IOException e) {
                throw new HaltPipelineException("Failed writing row to stream " + e.getMessage() );
            }
            return row;
        });

        after(out::close);
        return this;
    }

    public Pipeline clip(String... columns) {
        return addStep( renderName("clip", columns), ( Map<String,Object> row ) -> {
            Map<String,Object> result = new LinkedHashMap<>();
            for( String col : columns ) {
                result.put(col, row.get(col) );
            }
            return result;
        });
    }

    // todo this is not thread nice
    public Pipeline unique(String column) {
        Set<Object> unique = new HashSet<>();
        return addStep("unique(" + column + ")", (Map<String,Object> row) -> {
            if( unique.contains(row.get(column) ) ) return reject(row, "Non-unique row returned", RejectionCategory.IGNORE_ROW);
            unique.add( row.get(column) );
            return row;
        });
    }

    public Pipeline inject(String name, Function<Map<String,Object>, Iterable<Map<String,Object>>> closure) {
        Pipeline next = new Pipeline(name);
        next.src = new ChainedSource( this );
        addStep(name, ( Map<String,Object> row ) -> {
            Iterable<Map<String,Object>> result = closure.apply( row );
            if( result == null ) {
                row.put(REJECTED_KEY, new Rejection("Unknown reason", RejectionCategory.REJECTION ) );
                next.doRejections(row, name, -1);
                return row;
            } else {
                Iterable<Map<String,Object>> cc = result;
                ((ChainedSource)next.src).process( cc );
            }
            return row;
        });
        next.copyStatistics( this );
        return next;
    }

    public Pipeline inject(Function<Map<String,Object>,Iterable<Map<String,Object>>> closure) {
        return this.inject("inject()", closure );
    }

    public Pipeline filter(Predicate<Map<String,Object>> callback) {
        return addStep( "filter()", (Map<String,Object> row) -> {
            return callback.test(row) ? row : reject(row, "Row did not match the filter closure.", RejectionCategory.IGNORE_ROW );
        });
    }

    public Pipeline limit(long limit) {
        return limit( limit, true );
    }

    public Pipeline limit(long limit, boolean halt) {
        AtomicInteger current = new AtomicInteger(1);
        this.addStep("Limit(" + limit + ")", (Map<String,Object> row) -> {
            if( current.getAndIncrement() > limit ) {
                if( halt ) {
                    throw new HaltPipelineException("Over the maximum limit of " + limit);
                } else {
                    return reject(row, "Over the maximum limit of " + limit, RejectionCategory.IGNORE_ROW);
                }
            }
            return row;
        });
        return this;
    }

    public Pipeline defaultValues(Map<String,Object> defaults ) {
        return this.addStep(renderName("defaultValues", defaults.keySet() ), (Map<String,Object> row) -> {
            defaults.forEach( (String column, Object value) -> {
                if( !row.containsKey(column) ) row.put( column, value );
                if( row.get(column) == null || row.get(column).equals("") ) row.put( column, value );
            });
            return row;
        });
    }

    public Pipeline defaultsBy(Map<String,String> defaults ) {
        return this.addStep(renderName("defaultBy", defaults.keySet()), (Map<String,Object> row) -> {
            defaults.forEach( (String destColumn, String srcColumn) -> {
                if( !row.containsKey(destColumn) || row.get(destColumn) == null ) row.put(destColumn, row.get(srcColumn) );
            });
            return row;
        });
    }

    public void start() {
        this.start( null );
    }

    public void start(Function<Map<String,Object>,Map<String,Object>> closure) {
        try {
            if (closure != null) addStep("tail", closure);
            if( src != null ) {
                statistic.setStart(System.currentTimeMillis());
                src.start(this);
                statistic.setEnd(System.currentTimeMillis());

                runDoneChain();
            }
        } catch( HaltPipelineException ex ) {
            // ignore as we were asked to halt.
        }
        complete = true;
    }

    private void runDoneChain() {
        statistic.timed("Done Callbacks", () -> {
            doneChain.forEach(Runnable::run);
            return null;
        });
    }

    public LoadStatistic go() {
        start();
        return statistic;
    }

    public boolean process(Map<String,Object> row, int lineNumber) {
        Map<String,Object> next = row;
        for (Step<Map<String,Object>,Map<String,Object>> step : processChain) {
            try {
                Pair<Boolean,Map<String,Object>> pair = statistic.timed(step.name, next, (Map<String,Object> current) -> {
                    Map<String,Object> ret = step.step.apply(current);
                    if( ret == null || ret.containsKey(REJECTED_KEY) ) {
                        doRejections(ret == null ? reject(current, "Unknown reason") : ret, step.name, lineNumber);
                        return new Pair<>( true, ret );
                    }
                    return new Pair<>(false, ret );
                });
                if (pair.getKey()) return false;
                next = pair.getValue();
            } catch( HaltPipelineException ex ) {
                throw ex;
            } catch (Exception ex) {
                throw new RuntimeException("Line " + (lineNumber > 0 ? lineNumber : row) + ": Error encountered in step " + statistic.getName() + "." + step.name, ex);
            }
        }
        statistic.incrementLoaded();
        return false; // don't stop!
    }

    void copyStatistics(Pipeline src) {
        after( () -> {
            this.statistic.copy( src.statistic );
        });
    }

    public void doRejections(Map<String,Object> current, String stepName, int lineNumber) {
        Rejection rejection = (Rejection)current.remove(REJECTED_KEY);
        rejection.step = stepName;
        current.put("rejectionCategory", rejection.category);
        current.put("rejectionReason", rejection.reason);
        current.put("rejectionStep", rejection.step);
        statistic.reject(rejection);
        if (rejections != null) {
            rejections.process(current, lineNumber);
        }
    }

    public void setSrc(Source src) {
        this.src = src;
    }

    public boolean isRejected(Map<String,Object> row) {
        return row.containsKey(REJECTED_KEY);
    }

    private String renderName(String name, Collection<String> columns) {
        return columns.stream().collect(Collectors.joining(",", name + "(", ")"));
    }

    private String renderName(String name, String[] columns) {
        return Arrays.stream(columns).collect(Collectors.joining(",", name + "(", ")") );
    }

    public static Map<String,Object> reject(Map<String,Object> row, String reason) {
        row.put(REJECTED_KEY, new Rejection( reason, RejectionCategory.REJECTION ) );
        return row;
    }

    public static Map<String,Object> reject( Map<String,Object> row, String reason, RejectionCategory category ) {
        row.put( REJECTED_KEY, new Rejection( reason, category ) );
        return row;
    }
}
