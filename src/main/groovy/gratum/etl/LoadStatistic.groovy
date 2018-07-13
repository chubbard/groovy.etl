package gratum.etl

/**
 * Created by charliehubbard on 7/11/18.
 */
class LoadStatistic {
    String filename
    Map<RejectionCategory, Integer> rejectionsByCategory = [:]
    Map<String,Long> stepTimings = [:]
    Integer loaded = 0
    Long start = 0
    Long end = 0
    Long marker = 0

    public Duration getDuration() {
        return new Duration(end - start)
    }

    public long getElapsed() {
        return (System.currentTimeMillis() - marker)
    }

    public void mark() {
        marker = System.currentTimeMillis()
    }

    Long getStart() {
        return start
    }

    void setStart(Long start) {
        this.start = start
        mark()
    }

    Long getEnd() {
        return end
    }

    void setEnd(Long end) {
        this.end = end
    }

    public Integer getLoaded() {
        return loaded;
    }

    public Integer getRejections() {
        return rejectionsByCategory.inject(0) { Integer sum, RejectionCategory cat, Integer count -> sum + count }
    }

    public void reject(RejectionCategory category) {
        rejectionsByCategory[category] = (rejectionsByCategory[category] ?: 0) + 1
    }

    public Map<RejectionCategory, Integer> getRejectionsByCategory() {
        return rejectionsByCategory
    }

    public Integer getRejections(RejectionCategory category) {
        return rejectionsByCategory.get(category);
    }

    public Object timed( String stepName, Closure c ) {
        if( !stepTimings.containsKey(stepName) ) stepTimings.put( stepName, 0L )
        long start = System.currentTimeMillis()
        def ret = c.call()
        long duration = System.currentTimeMillis() - start
        stepTimings[stepName] += duration
        return ret
    }

    public double avg( String step ) {
        double avg = stepTimings[step] / (loaded + rejections)
        return avg
    }
}
