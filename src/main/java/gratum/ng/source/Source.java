package gratum.ng.source;

import gratum.ng.etl.Pipeline;

/**
 *
 */
public interface Source {

    void start(Pipeline pipeline );

    Pipeline into();
}
