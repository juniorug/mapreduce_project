/*
 * 
 */
package util;

import java.util.Comparator;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;



// TODO: Auto-generated Javadoc
/**
 * The Class UserComparator.
 */
public class UserComparator implements Comparator<UserComparator> {
    
    /** The name. */
    private String name;
    
    /** The reputation. */
    private Integer reputation;
    
    /** The count. */
    private Integer count;

    /**
     * Instantiates a new user comparator.
     *
     * @param name the name
     * @param reputation the reputation
     * @param count the count
     */
    public UserComparator(String name, Integer reputation, Integer count) {
        this.name = name;
        this.reputation = reputation;
        this.count = count;
           
    }

    /**
     * Gets the count.
     *
     * @return the count
     */
    public Integer getCount() {
        return count;
    }

    /**
     * Gets the reputation.
     *
     * @return the reputation
     */
    public Integer getReputation() {
        return reputation;
    }

    /**
     * Gets the name.
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
   
        return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers
                // if deriving: appendSuper(super.hashCode()).
                append(name).
                append(reputation).
                append(count).
                toHashCode();
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        
        if (!(obj instanceof UserComparator))
            return false;
        if (obj == this)
            return true;

        UserComparator comparator = (UserComparator) obj;
        return new EqualsBuilder().
            // if deriving: appendSuper(super.equals(obj)).
            append(name, comparator.name).
            append(reputation, comparator.reputation).
            append(count, comparator.count).
            isEquals();
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "UserComparator [Name = " + name + "], [Reputation = " + reputation + "], [Count = " + count + "]";
    }

    /* (non-Javadoc)
     * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
     */
    @Override
    public int compare(UserComparator o1, UserComparator o2) {
        int retorno = o1.getCount().compareTo(o2.getCount());
        if (retorno == 0) {
            return o1.getReputation().compareTo(o2.getReputation());
        } else {
            return retorno;
        }
    }

}