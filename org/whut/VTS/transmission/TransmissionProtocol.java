package org.whut.VTS.transmission;

import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface TransmissionProtocol extends VersionedProtocol{
	public static final long versionID=1L;
	void transmission(String splitOutputDir,String videoName,String splitOutputDirOnHDFS) throws IOException;
}
