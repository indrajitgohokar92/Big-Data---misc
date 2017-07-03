package JavaHDFS.JavaHDFS;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class ZipURLCopy {
	public static void main(String[] args) throws Exception {
		//Code to copy file from URL
		String urlLocation = args[0];
		String fileDestination = args[1];
		URL url =new URL(urlLocation);
		InputStream in = new BufferedInputStream(url.openStream());

		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));

		FileSystem fs = FileSystem.get(URI.create(fileDestination), conf);
		OutputStream out = fs.create(new Path(fileDestination), new Progressable() {
			public void progress() {
				System.out.print(".");
			}
		});
		System.out.println();
		IOUtils.copyBytes(in, out, 4096, true);

		//Code to unzip zip files
		
		Path intermediate = new Path(fileDestination);
	    FSDataInputStream fsInputStream = fs.open(intermediate);
	    ZipInputStream zipIn = new ZipInputStream(fsInputStream);
		ZipEntry entry = zipIn.getNextEntry();
		String finalDestination = fileDestination.substring(0, fileDestination.lastIndexOf("/"));
		
		//For unzipping each file.
		while (entry != null) {
			String filePath = finalDestination + File.separator + entry.getName();
			if (!entry.isDirectory()) {
				FSDataOutputStream fsOutputStream = fs.create(new Path(filePath));
				BufferedOutputStream bs = new BufferedOutputStream(fsOutputStream);
				byte[] bytesIn = new byte[4096];
				int read = 0;
				while ((read = zipIn.read(bytesIn)) != -1) {
					bs.write(bytesIn, 0, read);
				}
				bs.close();
			} 
			zipIn.closeEntry();
			entry = zipIn.getNextEntry();
		}
		zipIn.close();
		fs.delete(intermediate, true);
		fs.close();
	}
}
