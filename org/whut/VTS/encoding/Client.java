package org.whut.VTS.encoding;

import it.sauronsoftware.jave.AudioInfo;
import it.sauronsoftware.jave.Encoder;
import it.sauronsoftware.jave.EncoderException;
import it.sauronsoftware.jave.InputFormatException;
import it.sauronsoftware.jave.MultimediaInfo;
import it.sauronsoftware.jave.VideoInfo;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.whut.VTS.jobqueue.JobQueueProtocol;
import org.whut.VTS.merge.VideoMergeCmdWritable;
import org.whut.VTS.split.SplitProtocol;
import org.whut.VTS.split.VideoSplitCmdWritable;
import org.whut.VTS.transmission.TransmissionProtocol;
import org.whut.VTS.utils.ShareParamUtils;
import org.whut.VTS.utils.VTSUtil;


public class Client {

	private static class ProgressThread extends Thread{
		private int interruptTime;
		private boolean out=false;
		public ProgressThread(int interruptTime) {
			super();
			this.interruptTime = interruptTime;
		}
		@Override
		public void run() {
			// TODO Auto-generated method stub
			while(!out){
				try {
					Thread.sleep(interruptTime);
					System.out.print("=");
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		public void out(){
			this.out=true;
		}
	}
	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws EncoderException 
	 * @throws InputFormatException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, InputFormatException, EncoderException {
		// TODO Auto-generated method stub
		
		//待转码视频名字
		String videoName=args[0];
		
		//待转码视频文件目录
		String videoDir=args[1];
		
		//分割视频输出目录
		String splitOutputDir=args[2];
		
		//分割视频在HDFS中的目录
		String splitOutputDirOnHDFS=args[3];
		
		//合并视频输出目录
		String mergeVideoDir=args[4];
		
		//转换类型，只能选flv
		String type=args[5];
		
		//jobTrackerIP地址
		String jobTrackerhostname=args[6];
		
		
		//Only support flv
		
		File videoFile=new File(videoDir+"/"+videoName);
		
		MultimediaInfo mediaInfo=new Encoder().getInfo(videoFile);
		long videoSize=videoFile.length();
		
		long videoDuration=mediaInfo.getDuration();
		
		AudioInfo audioInfo=mediaInfo.getAudio();
		int bitRate=audioInfo.getBitRate();
		int channels=audioInfo.getChannels();
		int samplingRate=audioInfo.getSamplingRate();
		String adecoder=audioInfo.getDecoder();
		
		VideoInfo videoInfo=mediaInfo.getVideo();
		int totalBitRate=videoInfo.getBitRate();
		float frameRate=videoInfo.getFrameRate();
		int height=videoInfo.getSize().getHeight();
		int width=videoInfo.getSize().getWidth();
		String vdecoder=videoInfo.getDecoder();
		
		
		long startTime=0;
		
		System.out.println("*************************************待转码视频信息*************************************");
		System.out.println("视频名字: "+videoName+"                     视频大小: "+videoSize/(1024*1024)+"MB                     视频时长: "+videoDuration/1000+"秒");
		System.out.println("****************************************音频信息****************************************");
		System.out.println("比特率:"+bitRate+"           频道数: "+channels+"           音频采样频率: "+samplingRate+"kHz           "+"编码方式: "+adecoder);
		System.out.println("****************************************视频信息****************************************");
		System.out.println("总比特率:"+totalBitRate+"       帧频率: "+frameRate+"帧/秒          帧高度: "+height+"      帧宽度  :"+width+"     编码方式: "+vdecoder);
		
		System.out.println();
		System.out.println();
		
		Configuration conf=new Configuration();
		
		String localHostname="127.0.0.1";
		int splitRpcPort=ShareParamUtils.SPLITRPCPORT;
		InetSocketAddress splitRpcAddress=new InetSocketAddress(localHostname, splitRpcPort);
		SplitProtocol split=(SplitProtocol) RPC.getProxy(SplitProtocol.class, SplitProtocol.versionID, splitRpcAddress,conf);
		
		
		VideoSplitCmdWritable vsc=new VideoSplitCmdWritable("ffmpeg","-ss","-t","-i","",new String[]{"-vcodec","copy","-acodec","copy"});
		
		startTime=System.currentTimeMillis();
		System.out.println("****************************************分割开始****************************************");
		ProgressThread processThread=new ProgressThread(ShareParamUtils.INTERRUPTTIME);
		processThread.start();
		int splitNum=split.split(videoDir,videoName,splitOutputDir,videoFile.length(),videoDuration/1000,vsc);
		
		processThread.out();
		System.out.println();
		System.out.println("分割数量: "+splitNum+" 分割耗时: "+(System.currentTimeMillis()-startTime)/1000+"秒");
		System.out.println("****************************************分割完毕****************************************");
		
		System.out.println();
		System.out.println();
		
		int transmissionRpcPort=ShareParamUtils.TRANSMISSIONRPCPORT;
		InetSocketAddress transmissionRpcAddress=new InetSocketAddress(localHostname,transmissionRpcPort);
		TransmissionProtocol transmission=(TransmissionProtocol) RPC.getProxy(TransmissionProtocol.class, TransmissionProtocol.versionID, transmissionRpcAddress, conf);
		
		startTime=System.currentTimeMillis();
		System.out.println("****************************************传输开始****************************************");
		processThread=new ProgressThread(ShareParamUtils.INTERRUPTTIME);
		processThread.start();
		transmission.transmission(splitOutputDir, videoName,splitOutputDirOnHDFS);
		processThread.out();
		System.out.println();
		System.out.println("传输耗时: "+(System.currentTimeMillis()-startTime)/1000+"秒");
		System.out.println("****************************************传输结束****************************************");
		
		System.out.println();
		System.out.println();
		
		int jobQueueServerRpcPort=ShareParamUtils.JOBQUEUESERVERRPCPORT;
		InetSocketAddress jobQueueServerRpcAddress=new InetSocketAddress(localHostname,jobQueueServerRpcPort);
		JobQueueProtocol jobqueue=(JobQueueProtocol) RPC.getProxy(JobQueueProtocol.class, JobQueueProtocol.versionID, jobQueueServerRpcAddress, conf);
		
		VideoMergeCmdWritable mergecmd=new VideoMergeCmdWritable("mencoder", "-o", "", new String[]{"-ovc","copy","-oac","pcm"});
		VideoEncodingCmdWritable encodingcmd=new VideoEncodingCmdWritable("ffmpeg");
		
		int jobtrackerRpcPort=ShareParamUtils.JOBTRACKERRPCPORT;
		InetSocketAddress jobtrackerRpcAddress=new InetSocketAddress(jobTrackerhostname,jobtrackerRpcPort);
		JobClient jobClient=new JobClient(jobtrackerRpcAddress, conf);
		float mapProgress=0.1f;
		
		startTime=System.currentTimeMillis();
		EncodingAttributesWritable encodingAttributes=EncodingAttributesFactory.getEncodingAttributes(splitOutputDirOnHDFS, type);
		System.out.println("****************************************转码开始****************************************");
		TupleWritable tuple=jobqueue.summitTask(videoName,encodingAttributes,mergecmd,encodingcmd);
		
		JobID id=new JobID(tuple.getJtIdentifier(),tuple.getId());
		
		RunningJob rJob=jobClient.getJob(id);
		while(true){
			if(rJob.isComplete()){
				String state=rJob.isSuccessful()?"成功":"失败";
				System.out.println("转码耗时(包括等待作业队列为空的时间): "+(System.currentTimeMillis()-startTime)/1000+"秒"+"    状态: "+state);
				System.out.println("****************************************转码结束****************************************");
				break;
			}
			if(rJob.mapProgress()!=1&&rJob.mapProgress()>mapProgress){
				System.out.println(new Date()+" map: "+rJob.mapProgress()*100+"%");
				mapProgress+=0.1f;
				continue;
			}
			Thread.sleep(5000);
			rJob=jobClient.getJob(id);
		}
		System.out.println();
		System.out.println();
		
		FileSystem hdfs=FileSystem.get(conf);
		FileSystem local=FileSystem.getLocal(conf);
		hdfs.delete(new Path(splitOutputDirOnHDFS+"_reduce"),true);
		
		FileStatus[] files=hdfs.listStatus(new Path(splitOutputDirOnHDFS));
		String LocalDownloadDir=splitOutputDir+"_merge";
		String outputfile=mergeVideoDir+"/"+VTSUtil.getFileNameNoEx(videoName.toString())+"."+type;
		
		
		if(!rJob.isSuccessful()){
			hdfs.delete(new Path(splitOutputDirOnHDFS),true);
			return;
		}
		
		startTime=System.currentTimeMillis();
		System.out.println("****************************************合并开始****************************************");
		processThread=new ProgressThread(ShareParamUtils.INTERRUPTTIME);
		processThread.start();
		for(FileStatus file:files){
			if(!VTSUtil.getExtensionName(file.getPath().getName()).equals(type))
					continue;
			String fileName=file.getPath().getName();
			FSDataInputStream in=hdfs.open(file.getPath());
			IOUtils.copyBytes(in,local.create(new Path(LocalDownloadDir+"/"+fileName)), 2048,true);
		}
		hdfs.delete(new Path(splitOutputDirOnHDFS),true);
		
		List<String> arrayinputfiles=new ArrayList<String>();
		
		for(File inputfile:new File(LocalDownloadDir).listFiles()){
			if(!VTSUtil.getExtensionName(inputfile.getName()).equals(type))
						continue;
				arrayinputfiles.add(inputfile.toString());
		}
		
		String[] inputfiles=new String[arrayinputfiles.size()];
		for(int i=0;i<arrayinputfiles.size();i++)
			inputfiles[i]=arrayinputfiles.get(i);
			
		
		String cmd=VideoMergeCmdWritable.getCmdToString(mergecmd.getVideoMergeProcessPath(),mergecmd.getOutputFileCmd(),mergecmd.getInputFileCmd(),mergecmd.getArgs(),inputfiles, outputfile);
		Process pc=Runtime.getRuntime().exec(cmd);
		pc.waitFor();
		
		hdfs.delete(new Path(LocalDownloadDir), true);
		processThread.out();
		System.out.println("合并耗时: "+(System.currentTimeMillis()-startTime)/1000+"秒");
		System.out.println("****************************************合并结束****************************************");
		
		System.out.println();
		System.out.println();
		
		videoFile=new File(outputfile);
		mediaInfo=new Encoder().getInfo(videoFile);
		videoSize=videoFile.length();
		
		videoDuration=mediaInfo.getDuration();
		
		audioInfo=mediaInfo.getAudio();
		bitRate=audioInfo.getBitRate();
		channels=audioInfo.getChannels();
		samplingRate=audioInfo.getSamplingRate();
		adecoder=audioInfo.getDecoder();
		
		videoInfo=mediaInfo.getVideo();
		totalBitRate=videoInfo.getBitRate();
		frameRate=videoInfo.getFrameRate();
		height=videoInfo.getSize().getHeight();
		width=videoInfo.getSize().getWidth();
		vdecoder=videoInfo.getDecoder();
		
		System.out.println("*************************************转码后视频信息*************************************");
		System.out.println("视频名字: "+videoName+"                     视频大小: "+videoSize/(1024*1024)+"MB                     视频时长: "+videoDuration/1000+"秒");
		System.out.println("****************************************音频信息****************************************");
		System.out.println("比特率:"+bitRate+"           频道数: "+channels+"           音频采样频率: "+samplingRate+"kHz           "+"编码方式: "+adecoder);
		System.out.println("****************************************视频信息****************************************");
		System.out.println("总比特率:"+totalBitRate+"       帧频率: "+frameRate+"帧/秒          帧高度: "+height+"      帧宽度  :"+width+"     编码方式: "+vdecoder);
	}
}
