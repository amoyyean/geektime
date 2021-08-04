package hive.inoutformat;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class SQPTextInputFormat extends FileInputFormat<LongWritable, Text> implements JobConfigurable {

    private CompressionCodecFactory compressionCodecs = null;
    //"US-ASCII""ISO-8859-1""UTF-8""UTF-16BE""UTF-16LE""UTF-16"
    private final static String defaultEncoding = "UTF-8";
    private String encoding = null;

    public void configure(JobConf jobConf) {
        this.compressionCodecs = new CompressionCodecFactory(jobConf);
    }

    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        CompressionCodec codec = this.compressionCodecs.getCodec(filename);
        if (null == codec) {
            return true;
        }
        return codec instanceof SplittableCompressionCodec;
    }

    public RecordReader<LongWritable, Text> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
        reporter.setStatus(inputSplit.toString());
        String delimiter = jobConf.get("textinputformat.record.linesep");
        this.encoding = jobConf.get("textinputformat.record.encoding", defaultEncoding);
        byte[] recordDelimiterBytes = null;
        if (null != delimiter) {//Charsets.UTF_8
            recordDelimiterBytes = delimiter.getBytes(this.encoding);
        }
        return new SQPRecordReader(jobConf, (FileSplit) inputSplit, recordDelimiterBytes);
    }
}