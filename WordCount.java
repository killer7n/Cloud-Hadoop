import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static java.util.Arrays.*;

public class WordCount {
    static Collection<Text> anagrams = new HashSet<Text>();
    public static String stopWords="";
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{

        //private final static IntWritable one = new IntWritable(1);
        //private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String word = itr.nextToken();
                char[] wordarr = word.toCharArray();

                sort(wordarr);

                String wordanagram = new String(wordarr);

                stopWords = stopWords.replaceAll("\\p{Punct}", "").toLowerCase();
                word = word.replaceAll("\\p{Punct}", "").toLowerCase();
                if(stopWords.indexOf(word)==-1) {
                    context.write(new Text(wordanagram), new Text(word));
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        StringBuilder result = new StringBuilder();
        URL url = new URL("https://www.textfixer.com/tutorials/common-english-words-with-contractions.txt");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String line;
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }
        rd.close();
        stopWords =  result.toString();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(AnagramSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}