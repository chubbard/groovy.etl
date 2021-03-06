package gratum.ng.source;

import gratum.ng.etl.Pipeline;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/**
 * A source that uses a Collection&lt;Map&gt; as its source for rows.  For example,
 *
 * <pre>
 * from([
 *  [id: 1, name: 'Bill Rhodes', age: 53, gender: 'male'],
 *  [id: 2, name: 'Cheryl Lipscome', age: 43, gender: 'female'],
 *  [id: 3, name: 'Diana Rogers', age: 34, gender: 'female'],
 *  [id: 4, name: 'Jack Lowland', age: 25, gender: 'male'],
 *  [id: 5, name: 'Ginger Rogers', age: 83, gender: 'female']
 * ])
 * .filter({ Map row -&gt; row.age > 40 }
 * .go
 * </pre>
 */
class CollectionSource extends AbstractSource {

    private final Collection<Map<String,Object>> source;

    CollectionSource(Collection<Map<String,Object>> source) {
        this.name = "Collection";
        this.source = source;
    }

    @Override
    public void start(Pipeline pipeline) {
        int line = 1;
        for( Map<String,Object> r : source ) {
            pipeline.process( r, line++ );
        }
    }

    @SafeVarargs
    public static CollectionSource of(Map<String,Object>... src ) {
        return new CollectionSource( Arrays.asList( src ) );
    }

    public static CollectionSource of( Collection<Map<String,Object>> src ) {
        return new CollectionSource( src );
    }

    @SafeVarargs
    public static Pipeline from(Map<String,Object>... src) {
        Pipeline pipeline = new Pipeline("Array(" + src.length + ")");
        pipeline.setSrc( new CollectionSource( Arrays.asList(src) ) );
        return pipeline;
    }

    public static Pipeline from(Collection<Map<String,Object>> src ) {
        Pipeline pipeline = new Pipeline("Collection(" + src.size() + ")");
        pipeline.setSrc( new CollectionSource( src ) );
        return pipeline;
    }
}
