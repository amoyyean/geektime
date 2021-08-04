package hive.inoutformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hive.com.esotericsoftware.kryo.serializers.FieldSerializer;

import java.io.IOException;

public class GeekTextInputFormat implements InputFormat<LongWritable, Text>  , JobConfigurable{

//    org.apache.hive.com.esotericsoftware.kryo.serializers.FieldSerializer
    /**
     * GeekLineRecordReader
     *
     */
    public static class GeekLineRecordReader implements
            RecordReader<LongWritable, Text> {

        LineRecordReader reader;
        Text text;


        public GeekLineRecordReader(LineRecordReader reader) {
            this.reader = reader;
            text = reader.createValue();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

        @Override
        public LongWritable createKey() {
            return reader.createKey();
        }

        @Override
        public Text createValue() {
            return new Text();
        }

        @Override
        public long getPos() throws IOException {
            return reader.getPos();
        }

        @Override
        public float getProgress() throws IOException {
            return reader.getProgress();
        }

        @Override
        public boolean next(LongWritable key, Text value) throws IOException {
            while(reader.next(key, text)) {
                String strReplace =  text.toString().toLowerCase().replaceAll("ge{2,256}k", "");
                Text txtReplace = new Text();
                txtReplace.set(strReplace);
                value.set(txtReplace.getBytes(), 0, txtReplace.getLength());
                return true;
            }
                return false;
        }
    }

    TextInputFormat format;
    JobConf job;

    public GeekTextInputFormat() {
        format = new TextInputFormat();
    }

    @Override
    public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
        return format.getSplits(job, numSplits);
    }

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit genericSplit, JobConf jobConf, Reporter reporter) throws IOException {
            reporter.setStatus(genericSplit.toString());
            GeekTextInputFormat.GeekLineRecordReader reader = new GeekTextInputFormat.GeekLineRecordReader(
                    new LineRecordReader(job, (FileSplit) genericSplit));
            return reader;
    }

    @Override
    public void configure(JobConf job) {
        this.job = job;
        format.configure(job);
    }
}
