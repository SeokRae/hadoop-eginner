package wikibooks.hadoop.chapter03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SingleFileWriteRead {
	public static void main(String[] args) {
		if (args.length != 2) {
			System.err.println("Usage: SingleFileWriteRead <filename> <contents>");
			System.exit(2);
		}

		try {
			// create file system control instance 
			Configuration conf = new Configuration();
			FileSystem hdfs = FileSystem.get(conf);

			// path check
			Path path = new Path(args[0]);
			if (hdfs.exists(path)) {
				hdfs.delete(path, true);
			}

			// file store
			FSDataOutputStream outStream = hdfs.create(path);
			outStream.writeUTF(args[1]);
			outStream.close();

			// file output
			FSDataInputStream inputStream = hdfs.open(path);
			String inputString = inputStream.readUTF();
			inputStream.close();

			System.out.println("## inputString:" + inputString);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}