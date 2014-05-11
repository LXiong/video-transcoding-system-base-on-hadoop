package org.whut.VTS.split;

import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface SplitProtocol extends VersionedProtocol {
	
	public static final long versionID = 1L;

	//视频文件所在的目录,视频文件名字,视频文件分割后存放的目录,视频文件大小,视频持续时间,视频分割参数
	
	int split(String videoLocation, String videoName, String splitOutputdir,
			long videoSize, long videoDuration, VideoSplitCmdWritable vpc)
			throws IOException, InterruptedException;
}
