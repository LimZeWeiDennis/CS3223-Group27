package simpledb.materialize;

public interface Field {
    boolean isAggregate();
    /**
     * Return the name of the new field.
     * @return the name of the new field
     */
    String fieldName();
}
