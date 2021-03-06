package gratum.ng.etl;

import java.util.function.Function;

public class Step<T,R> {
    public String name;
    public Function<T,R> step;

    public Step(String name, Function<T, R> step) {
        this.name = name;
        this.step = step;
    }
}
