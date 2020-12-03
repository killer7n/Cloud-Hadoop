import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;

public class AnagramSumReducer
        extends Reducer<Text,Text,Text,Text> {


    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
        //ArrayList<Text> anagrams = new ArrayList<>();

        String anagram = null;
        for (Text val : values) {
            if (anagram == null) {

                anagram = val.toString();



            } else {

                anagram = anagram + ',' + val.toString();

            }

        }
        String[] anagrams = anagram.split(",");
        Arrays.sort(anagrams);
        String uniqueAnagrams=anagrams[0];
        String count="";
        int wordFrq=0;

        for (String word : anagrams) {
            if (uniqueAnagrams.indexOf(word) == -1) {
                uniqueAnagrams = uniqueAnagrams + ',' + word ;
                count = count+','+wordFrq;
                wordFrq = 1;
            } else {
                wordFrq++;
            }
        }
        count = count+','+wordFrq;
        count.substring(1);

        String[] uniqueAnagramList = uniqueAnagrams.split(",");
        String[] countList = count.split(",");
        String[] initialAnagramList = uniqueAnagrams.split(",");
        //String[] fullcountList = count.split(",");
        for(int i=0; i<uniqueAnagramList.length;i++){
            uniqueAnagramList[i]=uniqueAnagramList[i] + "["+countList[i+1]+"]";
        }
      // int size = count;
        if(uniqueAnagrams.split(",").length>1){
            context.write(new Text(initialAnagramList[0] ), new Text(Arrays.toString(uniqueAnagramList)));
        }
    }


}
