package edu.dartmouth.hmmem;

public class StringPair {
	
	public final String x;
	public final String y;
	
	public StringPair(String x, String y) {
		this.x = new String(x);
		this.y = new String(y);
	}
	
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof StringPair)) {
			return false;
		}
		StringPair other = (StringPair) o;
		return x.equals(other.x) && x.equals(other.x); 
	}
	
    @Override
    public int hashCode() {
        int hash = 7;
        hash = hash * 17 + x.hashCode();
        hash = hash * 31 + y.hashCode();
        return hash;
    }
    
    @Override
    public String toString() {
    	return "( " + x + " , " + y + " )";
    }
}
