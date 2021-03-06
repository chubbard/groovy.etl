package gratum.ng.source;

import gratum.ng.etl.Pipeline;

public abstract class AbstractSource implements Source {

    String name;

    @Override
    public Pipeline into() {
        Pipeline pipeline = new Pipeline( name );
        pipeline.setSrc( this );
        return pipeline;
    }
}
