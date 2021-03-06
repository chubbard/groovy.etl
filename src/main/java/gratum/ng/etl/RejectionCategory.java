package gratum.ng.etl;

/**
 * Created by charliehubbard on 7/11/18.
 */
public enum RejectionCategory {
    INVALID_FORMAT,
    MISSING_DATA,
    DUPLICATE,
    REJECTION,
    SCRIPT_ERROR,
    IGNORE_ROW
}