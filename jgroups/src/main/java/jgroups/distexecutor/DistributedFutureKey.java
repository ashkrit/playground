package jgroups.distexecutor;

import org.jgroups.Address;

public class DistributedFutureKey {

	private Address address;
	private int requestId;
	public DistributedFutureKey(Address source, int requestId) {
		super();
		this.address = source;
		this.requestId = requestId;
	}
	
	@Override
    public int hashCode() {
        int result = 0;
        result += ((address == null) ? 0 : address.hashCode());
        result +=  (int) (requestId ^ (requestId >>> 32));
        return result;
    }
  
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        DistributedFutureKey other = (DistributedFutureKey) obj;
        if (address == null) {
            if (other.address != null) return false;
        }
        else if (!address.equals(other.address)) return false;
        if (requestId != other.requestId) return false;
        return true;
    }	
}
