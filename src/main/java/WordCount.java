import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;


public class WordCount {

    public static class TokenizerFileMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        static enum CountersEnum { INPUT_WORDS } // 日志组名CountersEnum，日志名INPUT_WORDS

        private final static IntWritable one = new IntWritable(1); // map输出的value
        private Text word = new Text(); // map输出的key

        private Set<String> patternsToSkip = new HashSet<String>(); // 用来保存所有停用词 stopwords
        private Set<String> punctuations = new HashSet<String>(); // 用来保存所有要过滤的标点符号 stopwords

        private Configuration conf;
        private BufferedReader fis; // 保存文件输入流

        /**
         * 整个setup就做了两件事：
         * 1.读取配置文件中的wordcount.case.sensitive，赋值给caseSensitive变量
         * 2.读取配置文件中的wordcount.skip.patterns，如果为true，将CacheFiles的文件都加入过滤范围
         */
        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();


            // wordcount.skip.patterns属性的值取决于命令行参数是否有-skip，具体逻辑在main方法中
            if (conf.getBoolean("wordcount.skip.patterns", false)) { // 配置文件中的wordcount.skip.patterns功能是否打开
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles(); // getCacheFiles()方法可以取出缓存的本地化文件，本例中在main设置

                Path patternsPath = new Path(patternsURIs[0].getPath());
                String patternsFileName = patternsPath.getName().toString();
                parseSkipFile(patternsFileName); // 将文件加入过滤范围，具体逻辑参见parseSkipFile(String fileName)

                Path punctuationsPath = new Path(patternsURIs[1].getPath());
                String punctuationsFileName = punctuationsPath.getName().toString();
                parseSkipPunctuations(punctuationsFileName); // 将文件加入过滤范围，具体逻辑参见parseSkipFile(String fileName)
            }
        }

        /**
         * 用于在filename路径下读取文件，将文件中的需要过滤的停用词加入patternsToSkip
         * @param fileName
         */
        private void parseSkipFile(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) { // SkipFile的每一行都是一个需要过滤的pattern，例如\!
                    patternsToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file "
                        + StringUtils.stringifyException(ioe));
            }
        }

        /**
         * 用于在filename路径下读取文件，将文件中的需要过滤的标点符号加入punctuations
         * @param fileName
         */
        private void parseSkipPunctuations(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) { // SkipFile的每一行都是一个需要过滤的pattern，例如\!
                    punctuations.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file "
                        + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String textName = fileSplit.getPath().getName();

            String line = value.toString().toLowerCase(); // 因为作业要求忽略大小写，所以全转换成小写
            for (String pattern : punctuations) { // 将数据中所有满足patternsToSkip的pattern都过滤掉, replace by ""
                line = line.replaceAll(pattern, " ");
            }
            StringTokenizer itr = new StringTokenizer(line); // 将line以\t\n\r\f为分隔符进行分隔
            while (itr.hasMoreTokens()) {
                String one_word = itr.nextToken(); //从迭代器中读取单词

                //判断是否长度小于3
                if(one_word.length()<3) {
                    continue;
                }
                //判断是否是数字，用正则表达式
                if(Pattern.compile("^[-\\+]?[\\d]*$").matcher(one_word).matches()) {
                    continue;
                }
                //判断是否是停用词
                if(patternsToSkip.contains(one_word)){
                    continue;
                }

                word.set(one_word+"#"+textName); //key是单词+文件名，因为等会要分文件处理！
                context.write(word, one); //split word and count
                // getCounter(String groupName, String counterName)计数器
                // 枚举类型的名称即为组的名称，枚举类型的字段就是计数器名称
                Counter counter = context.getCounter(
                        CountersEnum.class.getName(),
                        CountersEnum.INPUT_WORDS.toString());
                counter.increment(1);
            }
        }
    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        static enum CountersEnum { INPUT_WORDS }

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private Set<String> patternsToSkip = new HashSet<String>();
        private Set<String> punctuations = new HashSet<String>();

        private Configuration conf;
        private BufferedReader fis;


        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();

            // wordcount.skip.patterns属性的值取决于命令行参数是否有-skip，具体逻辑在main方法中
            if (conf.getBoolean("wordcount.skip.patterns", false)) { // 配置文件中的wordcount.skip.patterns功能是否打开
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles(); // getCacheFiles()方法可以取出缓存的本地化文件，本例中在main设置

                Path patternsPath = new Path(patternsURIs[0].getPath());
                String patternsFileName = patternsPath.getName().toString();
                parseSkipFile(patternsFileName); // 将文件加入过滤范围，具体逻辑参见parseSkipFile(String fileName)

                Path punctuationsPath = new Path(patternsURIs[1].getPath());
                String punctuationsFileName = punctuationsPath.getName().toString();
                parseSkipPunctuations(punctuationsFileName); // 将文件加入过滤范围，具体逻辑参见parseSkipFile(String fileName)
            }
        }

        private void parseSkipFile(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) { // SkipFile的每一行都是一个需要过滤的pattern，例如\!
                    patternsToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file "
                        + StringUtils.stringifyException(ioe));
            }
        }

        private void parseSkipPunctuations(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) { // SkipFile的每一行都是一个需要过滤的pattern，例如\!
                    punctuations.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file "
                        + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().toLowerCase(); // 因为作业要求忽略大小写，所以全转换成小写
            for (String pattern : punctuations) { // 将数据中所有满足patternsToSkip的pattern都过滤掉, replace by ""
                line = line.replaceAll(pattern, " ");
            }
            StringTokenizer itr = new StringTokenizer(line); // 将line以\t\n\r\f为分隔符进行分隔
            while (itr.hasMoreTokens()) {
                String one_word = itr.nextToken(); //从迭代器中读取单词

                //判断是否长度小于3
                if(one_word.length()<3) {
                    continue;
                }
                //判断是否是数字，用正则表达式
                if(Pattern.compile("^[-\\+]?[\\d]*$").matcher(one_word).matches()) {
                    continue;
                }
                //判断是否是停用词
                if(patternsToSkip.contains(one_word)){
                    continue;
                }

                word.set(one_word); //key是单词+文件名，因为等会要分文件处理！
                context.write(word, one); //split word and count

                Counter counter = context.getCounter(
                        CountersEnum.class.getName(),
                        CountersEnum.INPUT_WORDS.toString());
                counter.increment(1);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static class SortFileReducer extends Reducer<IntWritable, Text, Text, NullWritable>{
        //将结果输出到多个文件或多个文件夹
        private MultipleOutputs<Text,NullWritable> mos;
        //创建对象
        protected void setup(Context context) throws IOException,InterruptedException {
            mos = new MultipleOutputs<Text, NullWritable>(context);
        }
        //关闭对象
        protected void cleanup(Context context) throws IOException,InterruptedException {
            mos.close();
        }

        private Text result = new Text();
        private HashMap<String, Integer> map = new HashMap<>();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            for(Text val: values){
                String docId = val.toString().split("#")[1];
                docId = docId.substring(0, docId.length()-4);
                docId = docId.replaceAll("-", "");
                String oneWord = val.toString().split("#")[0];
                int sum = map.values().stream().mapToInt(i->i).sum();
                //如果所有的value和加起来为40*100=4000，不干了，break
                if(sum==4000){
                    break;
                }
                //看看如果到100了，就跳过
                int rank = map.getOrDefault(docId, 0);
                if(rank == 100){
                    continue;
                }
                else {
                    rank += 1;
                    map.put(docId, rank); //0->1, n->n+1
                }
                result.set(oneWord.toString());
                String str=rank+": "+result+", "+key;
                mos.write(docId, new Text(str), NullWritable.get() );
            }
        }
    }

    public static class SortAllReducer extends Reducer<IntWritable, Text, Text, NullWritable>{
        private Text result = new Text();
        int rank=1;

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            for(Text val: values){
                if(rank > 100)
                {
                    break;
                }
                result.set(val.toString());
                String str=rank+": "+result+", "+key;
                rank++;
                context.write(new Text(str),NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);

        /**
         * 命令行语法是：hadoop command [genericOptions] [application-specific
         * arguments] getRemainingArgs()取到的只是[application-specific arguments]
         * 比如：$ bin/hadoop jar wc.jar WordCount2 input output -skip patterns.txt
         * getRemainingArgs() input output -skip patterns.txt
         */
        String[] remainingArgs = optionParser.getRemainingArgs();
        /**
         * remainingArgs.length == 2时，包括输入输出路径：
         * input output
         * remainingArgs.length == 5时，包括输入输出路径和跳过文件：
         * input output -skip patterns.txt stopword.txt
        */

        if ((remainingArgs.length != 2) && (remainingArgs.length != 5)) {
            System.err.println("Usage: wordcount <in> <out> [-skip punctuations skipPatternFile]");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerFileMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        //job.setPartitionerClass(NewPartitioner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        List<String> otherArgs = new ArrayList<String>(); // 除了 -skip 以外的其它参数
        for (int i = 0; i < remainingArgs.length; ++i) {
            if ("-skip".equals(remainingArgs[i])) {
                job.addCacheFile(new Path(remainingArgs[++i]).toUri()); // 将 -skip 后面的参数，即skip模式文件的url，加入本地化缓存中
                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job.getConfiguration().setBoolean("wordcount.skip.patterns", true); // 这里设置的wordcount.skip.patterns属性，在mapper中使用
            } else {
                otherArgs.add(remainingArgs[i]); // 将除了 -skip 以外的其它参数加入otherArgs中
            }
        }
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0))); // otherArgs的第一个参数是输入路径
        Path tempDir = new Path("tmp-file-word-count" ); //第一个job的输出写入临时目录，用于记录word#filename的词频
        FileOutputFormat.setOutputPath(job, tempDir);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        if(job.waitForCompletion(true))
        {
            //新建一个job处理排序和输出格式
            Job sortJob = Job.getInstance(conf, "sort file");
            sortJob.setJarByClass(WordCount.class);

            FileInputFormat.addInputPath(sortJob, tempDir);
            sortJob.setInputFormatClass(SequenceFileInputFormat.class);

            //map后交换key和value
            sortJob.setMapperClass(InverseMapper.class);
            sortJob.setReducerClass(SortFileReducer.class);

            Path tempFileDir = new Path("single-file-output" ); //第一个job的输出写入临时目录，用于记录word#filename的词频
            FileOutputFormat.setOutputPath(sortJob, tempFileDir);

            List<String> fileNameList = Arrays.asList("shakespearealls11", "shakespeareantony23", "shakespeareas12",
                    "shakespearecomedy7", "shakespearecoriolanus24", "shakespearecymbeline17", "shakespearefirst51",
                    "shakespearehamlet25", "shakespearejulius26", "shakespeareking45", "shakespearelife54",
                    "shakespearelife55", "shakespearelife56", "shakespearelovers62", "shakespeareloves8",
                    "shakespearemacbeth46", "shakespearemeasure13", "shakespearemerchant5", "shakespearemerry15",
                    "shakespearemidsummer16", "shakespearemuch3", "shakespeareothello47", "shakespearepericles21",
                    "shakespearerape61", "shakespeareromeo48", "shakespearesecond52", "shakespearesonnets59",
                    "shakespearesonnets", "shakespearetaming2", "shakespearetempest4", "shakespearethird53",
                    "shakespearetimon49", "shakespearetitus50", "shakespearetragedy57", "shakespearetragedy58",
                    "shakespearetroilus22", "shakespearetwelfth20", "shakespearetwo18", "shakespearevenus60",
                    "shakespearewinters19");

            for (String fileName : fileNameList) {
                MultipleOutputs.addNamedOutput(sortJob, fileName, TextOutputFormat.class,Text.class, NullWritable.class);
            }

            sortJob.setOutputKeyClass(IntWritable.class);
            sortJob.setOutputValueClass(Text.class);
            //排序改写成降序
            sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);


            if(sortJob.waitForCompletion(true)){
                Job job1 = Job.getInstance(conf, "all word count");
                job1.setJarByClass(WordCount.class);
                job1.setMapperClass(TokenizerMapper.class);
                job1.setCombinerClass(IntSumReducer.class);
                job1.setReducerClass(IntSumReducer.class);
                job1.setOutputKeyClass(Text.class);
                job1.setOutputValueClass(IntWritable.class);

                for (int i = 0; i < remainingArgs.length; ++i) {
                    if ("-skip".equals(remainingArgs[i])) {
                        job1.addCacheFile(new Path(remainingArgs[++i]).toUri()); // 将 -skip 后面的参数，即skip模式文件的url，加入本地化缓存中
                        job1.addCacheFile(new Path(remainingArgs[++i]).toUri());
                        job1.getConfiguration().setBoolean("wordcount.skip.patterns", true); // 这里设置的wordcount.skip.patterns属性，在mapper中使用
                    } else {
                        otherArgs.add(remainingArgs[i]); // 将除了 -skip 以外的其它参数加入otherArgs中
                    }
                }

                FileInputFormat.addInputPath(job1, new Path(otherArgs.get(0))); // otherArgs的第一个参数是输入路径
                Path tempAllDir = new Path("tmp-all-word-count" ); //第一个job的输出写入临时目录，用于记录word#filename的词频
                FileOutputFormat.setOutputPath(job1, tempAllDir);
                job1.setOutputFormatClass(SequenceFileOutputFormat.class);

                if(job1.waitForCompletion(true)){
                    //新建一个job处理排序和输出格式
                    Job sortJob1 = Job.getInstance(conf, "sort all");
                    sortJob1.setJarByClass(WordCount.class);

                    FileInputFormat.addInputPath(sortJob1, tempAllDir);

                    sortJob1.setInputFormatClass(SequenceFileInputFormat.class);

                    //map后交换key和value
                    sortJob1.setMapperClass(InverseMapper.class);
                    sortJob1.setReducerClass(SortAllReducer.class);

                    FileOutputFormat.setOutputPath(sortJob1, new Path(otherArgs.get(1)));

                    sortJob1.setOutputKeyClass(IntWritable.class);
                    sortJob1.setOutputValueClass(Text.class);

                    //排序改写成降序
                    sortJob1.setSortComparatorClass(IntWritableDecreasingComparator.class);

                    System.exit(sortJob1.waitForCompletion(true) ? 0 : 1);
                }
            }
        }
    }
}