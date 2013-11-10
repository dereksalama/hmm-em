package edu.dartmouth.hmmem;

public class StringPair {
	
	public final String x;
	public final String y;
	
	public StringPair(String x, String y) {
		this.x = x;
		this.y = y;
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
}
