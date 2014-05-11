package org.whut.VTS.encoding;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class VideoEncodingCmdWritable implements Writable{

	//视频转换程序路径
	private String VideoEncodingProcessPath;
	
	public VideoEncodingCmdWritable(String videoEncodingProcessPath) {
		super();
		VideoEncodingProcessPath = videoEncodingProcessPath;
	}

	public VideoEncodingCmdWritable() {
		super();
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		VideoEncodingProcessPath=arg0.readUTF();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeUTF(VideoEncodingProcessPath);
	}

}
