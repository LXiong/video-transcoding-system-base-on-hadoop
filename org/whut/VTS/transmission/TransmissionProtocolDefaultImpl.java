package org.whut.VTS.transmission;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.whut.VTS.utils.ShareParamUtils;

public class TransmissionProtocolDefaultImpl implements TransmissionProtocol {

	private String splitOutputDir;
	private String splitOutputDirOnHDFS;
	
private class TaskThread implements Runnable{
		
		private String fragment;
		
		public TaskThread(String fragment) {
			super();
			this.fragment = fragment;
		}

		public void run() {
			// TODO Auto-generated method stub
			try {
				Configuration conf=new Configuration();
				FileSystem hdfs=FileSystem.get(conf);
				FileSystem local=FileSystem.getLocal(conf);
				FSDataOutputStream out=hdfs.create(new Path(splitOutputDirOnHDFS+"/"+fragment));
				FSDataInputStream in=local.open(new Path(splitOutputDir+"/"+fragment));
				IOUtils.copyBytes(in, out, 2048, true);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	public void init() throws IOException{
		FileSystem hdfs=FileSystem.get(new Configuration());
		if(hdfs.exists(new Path(splitOutputDirOnHDFS)));
			hdfs.delete(new Path(splitOutputDirOnHDFS), true);
	}
	public void close() throws IOException{
		FileSystem local=FileSystem.getLocal(new Configuration());
		local.delete(new Path(splitOutputDir), true);
	}

	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		// TODO Auto-generated method stub
		return TransmissionProtocol.versionID;
	}

	@Override
	public void transmission(String splitOutputDir,String videoName,String splitOutputDirOnHDFS) throws IOException{
		// TODO Auto-generated method stub
		this.splitOutputDir=splitOutputDir;
		this.splitOutputDirOnHDFS=splitOutputDirOnHDFS;
		
		try {
			init();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		String[] fragments=new File(splitOutputDir).list();
		ExecutorService pool=Executors.newFixedThreadPool(ShareParamUtils.THREAD_NUM);
		
		for(String fragment:fragments){
			pool.execute(new TaskThread(new Path(fragment).getName()));
		}
		
		while(!pool.isTerminated())
			try {
				TimeUnit.SECONDS.sleep(5);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally{
				pool.shutdown();
			}
		
		try {
			close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String serverHost="127.0.0.1";
		int serverPort=ShareParamUtils.TRANSMISSIONRPCPORT;
		int numHandlers=ShareParamUtils.THREAD_NUM;
		Server server=RPC.getServer(new TransmissionProtocolDefaultImpl(), serverHost,serverPort,numHandlers,false,new Configuration());
		server.start();
	}

}
