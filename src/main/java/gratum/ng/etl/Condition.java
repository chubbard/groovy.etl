package gratum.ng.etl;

import groovy.lang.Closure;

import java.util.*;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

class Condition {

    public static class Builder {
        public static Condition eq(String col, Object value ) {
            return new Condition().eq( col, value );
        }

        public static Condition matches(String col, Pattern pattern ) {
            return new Condition().matches( col, pattern );
        }

        public static Condition any(String col, Collection<Object> values ) {
            return new Condition().any( col, values );
        }

        public static Condition when(String col, Predicate<Object> predicate ) {
            return new Condition().when( col, predicate );
        }

        public static Condition from(Map<String,Object> conds ) {
            return new Condition( conds );
        }
    }
    private final StringBuilder description = new StringBuilder();
    private final List<Predicate<Map<String,Object>>> comparators;

    private Condition() {
        comparators = new ArrayList<>();
    }

    public Condition(Map<String,Object> filterColumns) {
        comparators = new ArrayList<>(filterColumns.size());

        for( String col : filterColumns.keySet() ) {
            Object comp = filterColumns.get(col);
            if( comp instanceof Collection) {
                any(col, (Collection<Object>)comp );
            } else if( comp instanceof Predicate ) {
                when(col, (Predicate<Object>) comp);
            } else if( comp instanceof Closure) {
                when(col, (Object v) -> ((Closure<Boolean>)comp).call(v) );
            } else if( comp instanceof Pattern ) {
                matches( col, (Pattern)comp );
            } else {
                eq( col, comp );
            }
        }
    }

    protected Predicate<Map<String,Object>> createInCollectionCallback(String col, Collection<?> comp) {
        return (Map<String,Object> row) -> comp.contains(row.get(col));
    }

    protected Predicate<Map<String,Object>> createEqualsCallback(String col, Object comp) {
        return (Map<String,Object> row) -> row.containsKey(col) && row.get(col).equals(comp);
    }

    protected Predicate<Map<String,Object>> createPatternCallback(String col, Pattern pattern) {
        return (Map<String,Object> row) -> pattern.matcher( row.get(col).toString() ).find();
    }

    public boolean matches(Map<String,Object> row ) {
        for( Predicate<Map<String,Object>> c : comparators ) {
            if( !c.test(row) ) return false;
        }
        return true;
    }

    public Condition eq(String col, Object value) {
        toStart().append( col ).append( " = " ).append( value );
        comparators.add( createEqualsCallback(col, value) );
        return this;
    }

    public Condition any(String col, Collection<Object> values) {
        toStart().append( col ).append( " in " ).append(values.stream().map(Object::toString).collect( Collectors.joining(",", "[", "]") ) );
        comparators.add( createInCollectionCallback(col, values));
        return this;
    }

    public Condition matches(String col, Pattern pattern) {
        toStart().append( col ).append(" -> ").append( pattern.toString() );
        comparators.add( createPatternCallback(col, pattern));
        return this;
    }

    public Condition when(String col, Predicate<Object> predicate) {
        toStart().append( col ).append(" -> {}");
        comparators.add( (Map<String,Object> row) -> predicate.test( row.get(col) ) );
        return this;
    }

    private StringBuilder toStart() {
        if( description.length() == 0 ) return description;
        return description.append(", ");
    }

    @Override
    public String toString() {
        return description.toString();
    }
}
