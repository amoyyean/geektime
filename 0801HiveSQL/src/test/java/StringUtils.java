import java.util.Arrays;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtils {
    public static void main(String[] args) {
/*        String text = "This notebook can be geeeek used to geek install gek on all geeeek worker nodes, run data generation, and create the TPCDS geeeeeeeeek database." ;

        String strReplace = text.toString().toLowerCase().replaceAll("ge{2,256}k", "");
        System.out.println(strReplace );*/
        StringBuilder sb = new StringBuilder() ;
        String str = "This notebook can be used to geek gekk install gek on all worker nodes ,123 3523 12 4 13 123run data generation, and create the TPCDS database." ;
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
            randomNum -- ;
            if (flag){
                flag =  false ;
                sb.append(word) ;
            }
            else  {
                sb.append(" " + word) ;
            }
            if (0 == randomNum){
                randomNum = getRandomNum(min,max) ;
                // 生成gee...K
                String insertWord =  generateGeek(validWordCnt);
                sb.append(" " + insertWord);
                // 插入单词之后 ，有效单词总数从新开始计数
                validWordCnt = 0  ;
            }
        }
        System.out.println(sb.toString());
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


}
