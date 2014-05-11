package org.whut.VTS.split;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;


import org.whut.VTS.utils.ShareParamUtils;
import org.whut.VTS.utils.VTSUtil;

public class SplitProtocolDefaultImpl implements SplitProtocol{

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String serverHost="127.0.0.1";
		int serverPort=8181;
		int numHandlers=ShareParamUtils.THREAD_NUM;
		Server server=RPC.getServer(new SplitProtocolDefaultImpl(), serverHost,serverPort,numHandlers,false,new Configuration());
		server.start();
	}
	
	//视频分割命令
	private VideoSplitCmdWritable vpc;
	
	//视频大小
	private long videoSize;
	
	//视频持续时间
	private long videoDuration;
	
	//视频片段时间
	private long fragmentTime=0;
	
	//视频输出目录
	private String splitOutputDir;
			
	private List<Long> tasks=new ArrayList<Long>();
	
	
	private class taskThread implements Runnable{

		private long startTime;
		private long durationTime;
		private String inputfile;
		private String outputfile;
		
		public taskThread(long startTime, long durationTime, String inputfile,
				String outputfile) {
			super();
			this.startTime = startTime;
			this.durationTime = durationTime;
			this.inputfile = inputfile;
			this.outputfile = outputfile;
		}
		
		public void run() {
			// TODO Auto-generated method stub
			try {
				Process pc=Runtime.getRuntime().exec(vpc.getCmd(startTime, durationTime, inputfile, outputfile+"."+VTSUtil.getExtensionName(inputfile)));			
				pc.waitFor();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		// TODO Auto-generated method stub
		return SplitProtocol.versionID;
	}

	@Override
	public int split(String videoLocation, String videoName, String splitOutputdir,long videoSize, long videoDuration, VideoSplitCmdWritable vpc)
		throws IOException,InterruptedException{
		// TODO Auto-generated method stub
		this.videoSize =videoSize;
		this.videoDuration = videoDuration;
		this.vpc=vpc;
		
		this.splitOutputDir=splitOutputdir;
		
		init();
		ExecutorService pool=Executors.newFixedThreadPool(ShareParamUtils.THREAD_NUM);
		
		for(int i=0;i<tasks.size();i++)
			pool.execute(new taskThread(tasks.get(i), fragmentTime, videoLocation+"/"+videoName, splitOutputDir+"/"+i));
		
		pool.shutdown();
						
		while(!pool.isTerminated())
			try {
				TimeUnit.SECONDS.sleep(5);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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
