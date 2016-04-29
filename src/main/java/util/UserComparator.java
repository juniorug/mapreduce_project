package util;

import java.util.Comparator;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;



public class UserComparator implements Comparator<UserComparator> {
    
    private String name;
    private Integer id;
    private Integer count;

    public UserComparator(String name, Integer id, Integer count) {
        this.name = name;
        this.id = id;
        this.count = count;
           
    }

    public Integer getCount() {
        return count;
    }

    public Integer getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    @Override
    public int hashCode() {
   
        return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers
                // if deriving: appendSuper(super.hashCode()).
                append(name).
                append(id).
                append(count).
                toHashCode();
    }

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
            append(id, comparator.id).
            append(count, comparator.count).
            isEquals();
    }

    @Override
    public String toString() {
        return "UserComparator [Name = " + name + "], [Id = " + id + "], [Count = " + count + "]";
    }

    @Override
    public int compare(UserComparator o1, UserComparator o2) {
        int retorno = o1.getCount().compareTo(o2.getCount());
        if (retorno == 0) {
            return o1.getId().compareTo(o2.getId());
        } else {
            return retorno;
        }
    }

}