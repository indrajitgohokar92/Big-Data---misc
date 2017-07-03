package JavaHDFS.JavaHDFS;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.Progressable;

public class FileURLCopy {
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

		//Code to decompress bz2
		String uri = fileDestination;
		conf = new Configuration();
		fs = FileSystem.get(URI.create(uri), conf);

		Path inputPath = new Path(uri);
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		CompressionCodec codec = factory.getCodec(inputPath);
		if (codec == null) {
			System.err.println("No codec found for " + uri);
			System.exit(1);
		}

		String outputUri =
				CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());

		in = null;
		out = null;
		try {
			in = codec.createInputStream(fs.open(inputPath));
			out = fs.create(new Path(outputUri));
			IOUtils.copyBytes(in, out, conf);
		} finally {
			fs.delete(inputPath, true);
			fs.close();
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
		}

	}
}
