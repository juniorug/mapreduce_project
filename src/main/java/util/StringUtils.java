/*
 * 
 */
package util;

import org.apache.hadoop.io.Text;

// TODO: Auto-generated Javadoc
/**
 * The Class StringUtils.
 */
public class StringUtils {

	/**
	 * Formatter.
	 *
	 * @param text the text
	 * @return the string
	 */
	public static String formatter(Text text) {
    	String pipe = "|";
    	String name = "Name: ";
    	String reputation = "Reputation: " ;
    	String location = "Location: " ;
    	String[] userdata =  text.toString().split("\t")[0].split("\\|");
    	StringBuilder sb = new StringBuilder(name);
    	sb.append(pad(userdata[0], 21));
    	sb.append(pipe);
    	sb.append(reputation);
    	sb.append(pad(userdata[2], 5));
    	sb.append(pipe);
    	sb.append(location);
    	sb.append(pad(userdata[3], 29));
    	sb.append(pipe);
    	
    	return sb.toString();
    }
    
    /**
     * Pad.
     *
     * @param value the value
     * @param length the length
     * @return the string
     */
    private static String pad(String value, int length) {
        String with = " ";
        StringBuilder result = new StringBuilder(length);
        result.append(value);

        while (result.length() < length) {
           result.append(with); 
        }
        return result.toString();
    }
    
    /**
     * Gets the line.
     *
     * @return the line
     */
    public static String getLine() {
    	int length = 103;
    	String with = "-";
        StringBuilder result = new StringBuilder(length);
        result.append(with);

        while (result.length() < length) {
           result.append(with); 
        }
        return result.toString();
    	
    }
}
