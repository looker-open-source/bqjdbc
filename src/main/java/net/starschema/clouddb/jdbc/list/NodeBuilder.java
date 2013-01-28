package net.starschema.clouddb.jdbc.list;

/**
 * the interface, for generating the sql query output
 * 
 * @author Attila Horvath
 * 
 */
public interface NodeBuilder {
    /**
     * Returns the stored information in way we can use them to build our queries
     * Calling this will result in an untabbed result, for a human-readable result use
     * {@link #toPrettyString(int)}
     */
    public String toPrettyString();
    
    /** 
     *  Returns the stored information in way we can use them to build our queries in a
     *  human readable format
     *  @param level - how many tabulator should we use? -1 means no formatting
     */
    public String toPrettyString(int level);
}
