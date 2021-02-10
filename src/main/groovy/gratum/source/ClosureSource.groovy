package gratum.source

import gratum.etl.Pipeline
import groovy.transform.CompileStatic

@CompileStatic
class ClosureSource extends AbstractSource {

    Closure<Void> closure

    ClosureSource(Closure closure) {
        this.closure = closure
    }

    @Override
    void start(Pipeline pipeline) {
        this.closure.call(pipeline)
    }
}
