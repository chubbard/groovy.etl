package gratum.ng.etl;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class LoadStatistic {
    private final String name;
    private final Map<RejectionCategory, Map<String,Integer>> rejectionsByCategory = new HashMap<>();
    private Map<String,Long> stepTimings = new HashMap<>();
    private Integer loaded = 0;
    private Long start = 0L;
    private Long end = 0L;
    private Long marker = 0L;

    public LoadStatistic(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public Duration getDuration() {
        return new Duration(end - start);
    }

    public long getElapsed() {
        return (System.currentTimeMillis() - marker);
    }

    public void mark() {
        marker = System.currentTimeMillis();
    }

    Long getStart() {
        return start;
    }

    void setStart(Long start) {
        this.start = start;
        mark();
    }

    Long getEnd() {
        return end;
    }

    void setEnd(Long end) {
        this.end = end;
    }

    public Integer getLoaded() {
        return loaded;
    }

    public Integer getRejections() {
        return rejectionsByCategory.values().stream().map((Map<String,Integer> stepCounts) -> {
            return stepCounts.values().stream().mapToInt(Integer::intValue).sum();
        }).mapToInt(Integer::intValue).sum();
    }

    public void reject(Rejection rejection) {
        RejectionCategory category = rejection.category;
        if( !rejectionsByCategory.containsKey(category) ) {
            Map<String,Integer> x = new HashMap<>();
            x.put( rejection.step, 0 );
            rejectionsByCategory.put(category, x);
        } else if( !rejectionsByCategory.get(category).containsKey(rejection.step) ) {
            rejectionsByCategory.get(category).put(rejection.step, 0);
        }
        Integer count = rejectionsByCategory.get(category).get(rejection.step) + 1;
        rejectionsByCategory.get(category).put(rejection.step, count);
    }

    public Map<RejectionCategory, Map<String,Integer>> getRejectionsByCategory() {
        return rejectionsByCategory;
    }

    public Integer getRejections(RejectionCategory category) {
        return rejectionsByCategory.get(category).values().stream().mapToInt(Integer::intValue).sum();
    }

    public Integer getRejections(RejectionCategory cat, String step) {
        return rejectionsByCategory.get(cat).get(step);
    }

    public Map<String,Integer> getRejectionsFor( RejectionCategory category ) {
        return rejectionsByCategory.get(category);
    }

    public <R> R timed(String stepName, Supplier<R> c ) {
        if( !stepTimings.containsKey(stepName) ) stepTimings.put( stepName, 0L );
        long start = System.currentTimeMillis();
        R ret = c.get();
        long duration = System.currentTimeMillis() - start;
        stepTimings.put(stepName, stepTimings.get(stepName) + duration );
        return ret;
    }

    public <T,R> R timed( String stepName, T arg, Function<T,R> c ) {
        if( !stepTimings.containsKey(stepName) ) stepTimings.put( stepName, 0L );
        long start = System.currentTimeMillis();
        R ret = c.apply(arg);
        long duration = System.currentTimeMillis() - start;
        stepTimings.put(stepName, stepTimings.get(stepName) + duration );
        return ret;
    }

    public double avg( String step ) {
        double avg = stepTimings.get(step) / (double)(loaded + getRejections());
        return avg;
    }

    public String toString() {
        return toString(false);
    }

    public String toString(boolean timings) {
        StringWriter out = new StringWriter();
        PrintWriter pw = new PrintWriter(out);
        if( timings ) {
            pw.println("\n----");
            pw.println("Step Timings");
            this.stepTimings.forEach( (key, value) -> pw.printf("%s: %,.2f ms%n", key, this.avg(key)) );
        }

        if( this.getRejections() > 0 ) {
            pw.println("\n----");
            pw.println("Rejections by category");
            this.rejectionsByCategory.forEach( (RejectionCategory category, Map<String,Integer> steps) -> {
                pw.printf( "%s: %,d%n", category, steps.values().stream().mapToInt(Integer::intValue).sum() );
                steps.forEach( ( String step, Integer count ) -> {
                        pw.printf( "\t%s: %,d%n", step, count );
                });
            });
        }
        pw.println("\n----");
        pw.printf( "==> %s %nloaded %,d %nrejected %,d %ntook %,d ms%n", this.name, this.loaded, this.getRejections(),this.getElapsed() );
        return out.toString();
    }

    public void incrementLoaded() {
        this.loaded++;
    }

    public void copy(LoadStatistic src) {
        this.start = src.start;
        for( RejectionCategory cat : src.rejectionsByCategory.keySet() ) {
            if( !this.rejectionsByCategory.containsKey(cat) ) {
                this.rejectionsByCategory.put(cat, new HashMap<>());
            }
            for( String step : src.rejectionsByCategory.get(cat).keySet() ) {
                if( !this.rejectionsByCategory.get(cat).containsKey(step) ) {
                    this.rejectionsByCategory.get(cat).put(step, 0);
                }
                this.rejectionsByCategory.get(cat).put(step, this.rejectionsByCategory.get( cat ).get(step) + src.rejectionsByCategory.get( cat ).get(step) );
            }
        }

        Map<String,Long> timings = new HashMap<>();
        timings.putAll( src.stepTimings );
        timings.putAll( this.stepTimings );
        this.stepTimings = timings;
    }
}
