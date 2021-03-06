package gratum.ng.etl;

/**
 * Created by charliehubbard on 7/11/18.
 */
public class Rejection {

    public RejectionCategory category;
    public String reason;
    public String step;

    public Rejection(String reason) {
        this( reason, RejectionCategory.REJECTION );
    }

    public Rejection(String reason, RejectionCategory category ) {
        this( reason, category, null );
    }

    public Rejection(String reason, RejectionCategory category, String step) {
        this.category = category;
        this.reason = reason;
        this.step = step;
    }
}
