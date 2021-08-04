package hive.inoutformat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.io.InputStream;

/**
 * 自定义的 LineRecordReader 类
 * 此类是将org.apache.hadoop.mapred下的 LineRecordReader 源码拷贝进来进行改写。
 *
 */
public class SQPRecordReader  implements RecordReader<LongWritable, Text>  {

    private static final Log LOG = LogFactory.getLog(SQPRecordReader.class.getName());

    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private FSDataInputStream fileIn;
    private final Seekable filePosition;
    int maxLineLength;
    private CompressionCodec codec;
    private Decompressor decompressor;
    //field separator
    private String FieldSep;
    private static final String defaultFSep="\001";
    //"US-ASCII""ISO-8859-1""UTF-8""UTF-16BE""UTF-16LE""UTF-16"
    private final static String defaultEncoding = "UTF-8";
    private String encoding = null;
    public SQPRecordReader(Configuration job, FileSplit split) throws IOException {
        this(job, split, null);
    }
    public SQPRecordReader(Configuration job, FileSplit split, byte[] recordDelimiter) throws IOException {
        this.maxLineLength = job.getInt("mapreduce.input.linerecordreader.line.maxlength", 2147483647);
        this.FieldSep = job.get("textinputformat.record.fieldsep",defaultFSep);
        this.encoding = job.get("textinputformat.record.encoding",defaultEncoding);
        this.start = split.getStart();
        this.end = (this.start + split.getLength());
        Path file = split.getPath();
        this.compressionCodecs = new CompressionCodecFactory(job);
        this.codec = this.compressionCodecs.getCodec(file);
        FileSystem fs = file.getFileSystem(job);
        this.fileIn = fs.open(file);
        if (isCompressedInput()) {
            this.decompressor = CodecPool.getDecompressor(this.codec);
            if ((this.codec instanceof SplittableCompressionCodec)) {
                SplitCompressionInputStream cIn = ((SplittableCompressionCodec)this.codec).createInputStream(this.fileIn, this.decompressor, this.start, this.end, SplittableCompressionCodec.READ_MODE.BYBLOCK);
                this.in = new LineReader(cIn, job, recordDelimiter);
                this.start = cIn.getAdjustedStart();
                this.end = cIn.getAdjustedEnd();
                this.filePosition = cIn;
            } else {
                this.in = new LineReader(this.codec.createInputStream(this.fileIn, this.decompressor), job, recordDelimiter);
                this.filePosition = this.fileIn;
            }
        } else {
            this.fileIn.seek(this.start);
            this.in = new LineReader(this.fileIn, job, recordDelimiter);
            this.filePosition = this.fileIn;
        }
        if (this.start != 0L) {
            this.start += this.in.readLine(new Text(), 0, maxBytesToConsume(this.start));
        }
        this.pos = this.start;
    }
    public SQPRecordReader(InputStream in, long offset, long endOffset, int maxLineLength) {
        this(in, offset, endOffset, maxLineLength, null);
    }
    public SQPRecordReader(InputStream in, long offset, long endOffset, int maxLineLength, byte[] recordDelimiter) {
        this.maxLineLength = maxLineLength;
        this.in = new LineReader(in, recordDelimiter);
        this.start = offset;
        this.pos = offset;
        this.end = endOffset;
        this.filePosition = null;
    }
    public SQPRecordReader(InputStream in, long offset, long endOffset, Configuration job) throws IOException {
        this(in, offset, endOffset, job, null);
    }
    public SQPRecordReader(InputStream in, long offset, long endOffset, Configuration job, byte[] recordDelimiter) throws IOException {
        this.maxLineLength = job.getInt("mapreduce.input.linerecordreader.line.maxlength", 2147483647);
        this.in = new LineReader(in, job, recordDelimiter);
        this.start = offset;
        this.pos = offset;
        this.end = endOffset;
        this.filePosition = null;
    }
    public LongWritable createKey() {
        return new LongWritable();
    }
    public Text createValue() {
        return new Text();
    }
    private boolean isCompressedInput() {
        return this.codec != null;
    }
    private int maxBytesToConsume(long pos) {
        return isCompressedInput() ? 2147483647 : (int)Math.min(2147483647L, this.end - pos);
    }
    private long getFilePosition() throws IOException {
        long retVal;
        if ((isCompressedInput()) && (null != this.filePosition))
            retVal = this.filePosition.getPos();
        else {
            retVal = this.pos;
        }
        return retVal;
    }
    public boolean next(LongWritable longWritable, Text text) throws IOException {
        while (getFilePosition() <= this.end) {
            longWritable.set(this.pos);
            int newSize = this.in.readLine(text, this.maxLineLength, Math.max(maxBytesToConsume(this.pos), this.maxLineLength));
            if (newSize == 0) {
                return false;
            }
            if (encoding.compareTo(defaultEncoding) != 0) {
                String str = new String(text.getBytes(), 0, text.getLength(), encoding);
                text.set(str);
            }
            if (FieldSep.compareTo(defaultFSep) != 0) {
                String replacedValue = text.toString().replace(FieldSep, defaultFSep);
                text.set(replacedValue);
            }
            this.pos += newSize;
            if (newSize < this.maxLineLength) {
                return true;
            }
            LOG.info("Skipped line of size " + newSize + " at pos " + (this.pos - newSize));
        }
        return false;
    }
    public long getPos() throws IOException {
        return this.pos;
    }
    public void close() throws IOException {
        try {
            if (this.in != null)
                this.in.close();
        }
        finally {
            if (this.decompressor != null)
                CodecPool.returnDecompressor(this.decompressor);
        }
    }
    public float getProgress() throws IOException {
        if(this.start == this.end){
            return 0.0F;
        }
        return Math.min(1.0F, (float)(getFilePosition() - this.start) / (float)(this.end - this.start));
    }
}