package jgroups.distexecutor.request;

import java.io.DataInput;
import java.io.DataOutput;

import org.jgroups.Header;

public class RequestTypeHeader extends Header
{
	public RequestTypeHeader(){}
	
	public void setType(byte type) {
		this.type = type;
	}
	
	public byte getType() {
		return type;
	}
	
	private byte type;
	public RequestTypeHeader(byte type)
	{
		this.type = type;
	}
	public void writeTo(DataOutput out) throws Exception {
		out.writeByte(type);				
	}
	
	public void readFrom(DataInput in) throws Exception {
		type = in.readByte();				
	}
	
	@Override
	public int size() {
		return 1;
	}
}
