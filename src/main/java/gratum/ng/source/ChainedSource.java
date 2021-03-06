package gratum.ng.source;

import gratum.ng.etl.Pipeline;

import java.util.Map;

public class ChainedSource extends AbstractSource {

    private final Pipeline parent;
    private Pipeline delegate;
    int line = 1;

    public ChainedSource(Pipeline parent) {
        this.name = parent.getName();
        this.parent = parent;
    }

    @Override
    public void start(Pipeline pipeline) {
        this.delegate = pipeline;
        parent.start();
    }

    public void process( Map<String,Object> row ) {
        this.delegate.process( row, line++ );
    }

    public void process( Iterable<Map<String,Object>> rows ) {
        for( Map<String,Object> r : rows ) {
            if( this.delegate.isRejected(r) ) {
                this.delegate.doRejections( r, name, line );
            } else {
                this.delegate.process( r, line++ );
            }
        }
    }
}
