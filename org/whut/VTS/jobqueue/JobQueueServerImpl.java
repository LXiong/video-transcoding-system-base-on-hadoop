package org.whut.VTS.jobqueue;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.whut.VTS.encoding.EncodingAttributesWritable;
import org.whut.VTS.encoding.EncodingInputFormat;
import org.whut.VTS.encoding.EncodingUsingJAVEMapper;
import org.whut.VTS.encoding.TupleWritable;
import org.whut.VTS.encoding.VideoEncodingCmdWritable;
import org.whut.VTS.merge.VideoMergeCmdWritable;
import org.whut.VTS.utils.ShareParamUtils;


public class JobQueueServerImpl implements JobQueueProtocol{

	
	private static class JobTuple{
		public TupleWritable jobId;
		public Job job;
		public JobTuple(TupleWritable jobId, Job job) {
			super();
			this.jobId = jobId;
			this.job = job;
		}
	}
	
	
	
	static BlockingQueue<JobTuple> jobQueue=new LinkedBlockingQueue<JobTuple>(ShareParamUtils.JOBCAPACITY);
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		//启动一个Daemon线程
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
				while(true){
					JobTuple jobTuple;
					try {
						jobTuple = jobQueue.poll(Long.MAX_VALUE,TimeUnit.SECONDS);
						new JobThread(jobTuple).run();
	
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}).start();
		
		String serverHost="127.0.0.1";
		int serverPort=ShareParamUtils.JOBQUEUESERVERRPCPORT;
		int numHandlers=ShareParamUtils.THREAD_NUM;
		Server server=RPC.getServer(new JobQueueServerImpl(), serverHost,serverPort,numHandlers,false,new Configuration());
		server.start();
		
	}

	private static class JobThread implements Runnable{
		private JobTuple jobTuple;
		
		public JobThread(JobTuple jobTuple) {
			super();
			/*this.attrs = attrs;*/
			this.jobTuple=jobTuple;
		}
		
		@Override
		public void run() {
			// TODO Auto-generated method stub
			try {
					jobTuple.job.submit();
					synchronized (jobTuple.jobId){
						JobID id=jobTuple.job.getJobID();
						jobTuple.jobId.setJtIdentifier(id.getJtIdentifier());
						jobTuple.jobId.setId(id.getId());
						jobTuple.jobId.notify();
					}
					
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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
		return JobQueueProtocol.versionID;
	}

	@Override
	public TupleWritable summitTask(String videoName,EncodingAttributesWritable attrs,VideoMergeCmdWritable mergecmd,VideoEncodingCmdWritable encodingcmd) throws IOException {
		// TODO Auto-generated method stub
		Configuration conf=new Configuration();
		
		//视频名字
		conf.set("videoName",videoName);
		
		//转码信息
		conf.set("splitDir",attrs.getSplitDirOnHDFS());
		conf.set("audioCodec",attrs.getAudioCodec());
		conf.setInt("audioBitRate", attrs.getAudioBitRate());
		conf.setInt("audioChannels", attrs.getAudioChannels());
		conf.setInt("audioSamplingRate", attrs.getAudioSamplingRate());
		
		conf.set("videoCodec", attrs.getVideoCodec());
		conf.setInt("videoBitRate", attrs.getVideoBitRate());
		conf.setInt("videoFrameRate", attrs.getVideoFrameRate());
		conf.setInt("videoLength", attrs.getVideoLength());
		conf.setInt("videoWidth", attrs.getVideoWidth());
		
		conf.set("videoDirOnHDFS",attrs.getSplitDirOnHDFS());
		
		conf.set("localdownloadDir","tmp");
		
		//视频名字
		conf.set("videoName",videoName);
		
		
		//合并参数
		conf.set("VideoMergeProcessPath", mergecmd.getVideoMergeProcessPath());
		conf.set("OutputFileCmd", mergecmd.getOutputFileCmd());
		conf.set("InputFileCmd", mergecmd.getInputFileCmd());
		conf.set("args", mergecmd.getArgs());
		
		//作业配置相关类
		Job job=new Job(conf);
		
		job.setJobName("videoCodec_"+videoName);
		job.setJarByClass(JobQueueServerImpl.class);
		
		job.setInputFormatClass(EncodingInputFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
				
		job.setMapperClass(EncodingUsingJAVEMapper.class);
		
		job.setNumReduceTasks(0);
		FileInputFormat.setInputPaths(job, attrs.getSplitDirOnHDFS());
		FileOutputFormat.setOutputPath(job, new Path(attrs.getSplitDirOnHDFS()+"_reduce"));
				
		
		TupleWritable jobId=new TupleWritable("",0);
		try {
			jobQueue.put(new JobTuple(jobId, job));
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		synchronized (jobId) {			
				try {
					while(jobId.getId()==0){
								jobId.wait();
					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		return new TupleWritable(jobId.getJtIdentifier(),jobId.getId());
	}
}
