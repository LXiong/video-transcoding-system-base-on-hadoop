package org.whut.VTS.split;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.whut.VTS.utils.ShareParamUtils;
import org.whut.VTS.utils.VTSUtil;



public class SplitProtocolNoThreadImpl implements SplitProtocol{


	private VideoSplitCmdWritable vpc;
	

	private long videoSize;

	private long videoDuration;

	private long fragmentTime=0;
	
	private String splitOutputDir;
	
	
	
	
	private List<Long> tasks=new ArrayList<Long>();
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String serverHost="127.0.0.1";
		int serverPort=8181;
		int numHandlers=ShareParamUtils.THREAD_NUM;
		Server server=RPC.getServer(new SplitProtocolNoThreadImpl(), serverHost,serverPort,numHandlers,false,new Configuration());
		server.start();
	}

	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		// TODO Auto-generated method stub
		return SplitProtocol.versionID;
	}

	@Override
	public int split(String videoLocation, String videoName,String splitOutputdir,
			long videoSize, long videoDuration, VideoSplitCmdWritable vpc)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		this.videoSize =videoSize;
		this.videoDuration = videoDuration;
		this.vpc=vpc;
		
		splitOutputDir=splitOutputdir;
		init();
		
		Process pc;
		for(int i=0;i<tasks.size();i++){
			pc=Runtime.getRuntime().exec(this.vpc.getCmd(tasks.get(i), fragmentTime, videoLocation+"/"+videoName, splitOutputDir+"/"+i+"."+VTSUtil.getExtensionName(videoName)));
			pc.waitFor();
		}
		return tasks.size();
	}
	
	public void init(){
		// TODO Auto-generated method stub
		fragmentTime=Math.round(videoDuration/(videoSize/ShareParamUtils.FRAGMENT_SIZE));
		for(long i=0;i<videoDuration;i+=fragmentTime)
					tasks.add(i);
		if(!new File(splitOutputDir).exists())
					new File(splitOutputDir).mkdir();
	}

}
