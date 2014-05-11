package org.whut.VTS.encoding;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TupleWritable implements Writable{

	private String jtIdentifier;
	private int id;
	
	
	public TupleWritable(String jtIdentifier, int id) {
		super();
		this.jtIdentifier = jtIdentifier;
		this.id = id;
	}

	public TupleWritable() {
		super();
	}

	public String getJtIdentifier() {
		return jtIdentifier;
	}

	public void setJtIdentifier(String jtIdentifier) {
		this.jtIdentifier = jtIdentifier;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		jtIdentifier=arg0.readUTF();
		id=arg0.readInt();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeUTF(jtIdentifier);
		arg0.writeInt(id);
	}

}
