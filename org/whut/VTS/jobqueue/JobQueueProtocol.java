package org.whut.VTS.jobqueue;



import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;
import org.whut.VTS.encoding.EncodingAttributesWritable;
import org.whut.VTS.encoding.TupleWritable;
import org.whut.VTS.encoding.VideoEncodingCmdWritable;
import org.whut.VTS.merge.VideoMergeCmdWritable;

public interface JobQueueProtocol extends VersionedProtocol{
	public static final long versionID=1L;
	TupleWritable summitTask(String videoName,EncodingAttributesWritable attrs,VideoMergeCmdWritable mergecmd,VideoEncodingCmdWritable encodingcmd) throws IOException;
}
