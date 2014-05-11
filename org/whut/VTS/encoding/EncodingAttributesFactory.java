package org.whut.VTS.encoding;

public class EncodingAttributesFactory {
	private EncodingAttributesFactory(){
		
	}
	public static EncodingAttributesWritable getEncodingAttributes(String splitDirOnHDFS,String format){
			return new EncodingAttributesWritable(splitDirOnHDFS, "libmp3lame", 64000, 1, 22050, format, 160000, 15, 400, 300);
	}
}
