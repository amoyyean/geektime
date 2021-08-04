package hive.inoutformat;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GeekTextOutputFormat <K extends WritableComparable, V extends Writable>
        extends HiveIgnoreKeyTextOutputFormat<K, V> {
    /**
     * GeekRecordWriter.
     *
     */
    public static class GeekRecordWriter implements FileSinkOperator.RecordWriter{

        FileSinkOperator.RecordWriter writer;
        Text text ;

        public GeekRecordWriter(FileSinkOperator.RecordWriter writer) {
            this.writer = writer;
            text = new Text() ;
//            bytesWritable = new BytesWritable();
        }

        @Override
        public void write(Writable w) throws IOException {
            // Get input data
            Text tmpText = ((Text) w);
            StringBuilder sb = new StringBuilder() ;
//            String str = "This notebook can be used to geek gekk install gek on all worker nodes ,123 3523 12 4 13 123run data generation, and create the TPCDS database." ;
            String str =  tmpText.toString() ;
            String[] words = str.toString().toLowerCase().split("\\s+") ;
            // 生成一个 min-max 之间的随机整数
            int min = 2  ;
            int max = 6 ;
            int randomNum = getRandomNum(min,max) ;
            int validWordCnt = 0 ;
            boolean flag = true ;
            for (String word: words) {
                // 如果是有效单词， 统计数+1 ， 方便后面生成gee...K , 生成中间e的个数使用
                if(checkValidWord(word)){
                    validWordCnt++ ;
                }
                // 随机数个单词标记
                randomNum -- ;
                // 行首单词不加空格
                if (flag){
                    flag =  false ;
                    sb.append(word) ;
                }
                else  {
                    // 非行首单词输出空格+单词
                    sb.append(" " + word) ;
                }
                // 输出指定生成随机数个单词后，进行插入操作
                if (0 == randomNum){
                    // 生成指定min-max区间的随机整数
                    randomNum = getRandomNum(min,max) ;
                    // 利用之前产生的有效单词数，生成gee...K
                    String insertWord =  generateGeek(validWordCnt);
                    sb.append(" " + insertWord);
                    // 插入单词之后 ，有效单词总数从新开始计数
                    validWordCnt = 0  ;
                }
            }
            String strReplace =  sb.toString();
            Text output = new Text();
            output.set(strReplace);
            text.set(output.getBytes(), 0, output.getLength());
            writer.write(text);
        }
        public static int getRandomNum(int min , int max){
            return (int) (Math.random()*(max-min)+min) ;
        }

        public static String generateGeek(int eCnt){
            String prefix = "g" ;
            String loopStr = "e" ;
            String suffix = "k" ;
            StringBuilder sb = new StringBuilder(prefix) ;
            for (int i = 0; i < eCnt; i++) {
                sb.append(loopStr) ;
            }
            return sb.append(suffix).toString();
        }

        public static boolean checkValidWord(String word){
            boolean res = true;

            String tmp = word;
            tmp = tmp.replaceAll("\\p{P}", "");
            if (word.length() != tmp.length()) {
                res = false;
            }
            // 定义模式匹配
            Pattern pattern = Pattern.compile("ge{2,256}k");
            Matcher matcher = pattern.matcher(word) ;
            // matches 进行全局匹配 与给定的字符串进行全匹配
            // 测试word 中包含geek , 返回false
            res = !matcher.matches();
            return res;
        }

        @Override
        public void close(boolean abort) throws IOException {
            writer.close(abort);
        }

    }

    @Override
    public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
                                                             Class<? extends Writable> valueClass, boolean isCompressed,
                                                             Properties tableProperties, Progressable progress) throws IOException {

        GeekTextOutputFormat.GeekRecordWriter writer = new GeekTextOutputFormat.GeekRecordWriter(super
                .getHiveRecordWriter(jc, finalOutPath, BytesWritable.class,
                        isCompressed, tableProperties, progress));
        return writer;
    }
}
