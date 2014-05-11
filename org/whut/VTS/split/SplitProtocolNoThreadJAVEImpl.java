package org.whut.VTS.split;

import it.sauronsoftware.jave.AudioAttributes;
import it.sauronsoftware.jave.Encoder;
import it.sauronsoftware.jave.EncoderException;
import it.sauronsoftware.jave.EncodingAttributes;
import it.sauronsoftware.jave.InputFormatException;
import it.sauronsoftware.jave.VideoAttributes;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.whut.VTS.utils.ShareParamUtils;
import org.whut.VTS.utils.VTSUtil;




public class SplitProtocolNoThreadJAVEImpl implements SplitProtocol {

	
	
	//视频文件大小
	private long videoSize;
	//视频文件时长
	private long videoDuration;
	//每片视频时长
	private long fragmentTime=0;
	
	private String splitOutputDir;
	
	
	
	//任务列表
	private List<Long> tasks=new ArrayList<Long>();
	
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
		
		splitOutputDir=splitOutputdir;
		
		
		init();
		
	
		EncodingAttributes attrs=new EncodingAttributes();
		attrs.setDuration((float)fragmentTime);
		
		attrs.setFormat(VTSUtil.getExtensionName(videoName));
		
		AudioAttributes audioattr=new AudioAttributes();
		audioattr.setCodec(AudioAttributes.DIRECT_STREAM_COPY);
		attrs.setAudioAttributes(audioattr);
		
		VideoAttributes videoattr=new VideoAttributes();
		videoattr.setCodec(VideoAttributes.DIRECT_STREAM_COPY);
		attrs.setVideoAttributes(videoattr);
		Encoder encoder=new Encoder();
		
		File source=new File(videoLocation+"/"+videoName);
		for(int i=0;i<tasks.size();i++){
			File target=new File(splitOutputDir+"/"+i+"."+VTSUtil.getFileNameNoEx(videoName));
			attrs.setOffset((float)tasks.get(i));
			
			try {
				encoder.encode(source, target, attrs);
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InputFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (EncoderException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return tasks.size();
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String serverHost="127.0.0.1";
		int serverPort=8181;
		int numHandlers=ShareParamUtils.THREAD_NUM;
		Server server=RPC.getServer(new SplitProtocolNoThreadJAVEImpl(), serverHost,serverPort,numHandlers,false,new Configuration());
		server.start();
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
