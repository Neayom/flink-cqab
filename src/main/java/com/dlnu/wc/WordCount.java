package com.dlnu.wc;

import com.dlnu.Cache.Cache;
import com.dlnu.Cache.CacheManager;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author qimingchen on 8/23/21
 * @Project flink-demo
 * @Description: Demo for batch processing data set
 */
public class WordCount {
    //参数1 故障点 参数2 输入 参数3输出
    static boolean isError = false;
    static String signal = "main";
    static String str = "44d57c7d-09b8-4e63-9efb-c9af588f1798"; //模拟故障的点
    static String ErrorTime;//故障发生的时间
    static String correctTime;//故障解决的时间
    public static void main(String[] args) throws Exception {

        // create execution env
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        //read data from file
        String inputPath = "src/main/resources/Wordcountpre.txt";
        //it is extending DataSet
        //DataSet<String> stringDataSource = executionEnvironment.readTextFile(inputPath);
        DataSource<String> inputDataSet = executionEnvironment.readTextFile(inputPath);
        DataSource<String> subinputDataSet = executionEnvironment.readTextFile(inputPath);
        MapOperator<String, String> mapMain = inputDataSet.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                if (s.split(" ")[0].equals(str)) {
                    isError = true;
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYY-MM-dd:HH:mm:ss:SSS");
                    ErrorTime = simpleDateFormat.format(new Date());
                    System.out.println("主算子处理"+str+"过程中出现异常交由备份算子处理");
                    Thread.sleep(30201);
                    return "error "+s;
                } else {
                    return s.toString();
                }
            }
        });
        MapOperator<String, String> mapSubMain = subinputDataSet.map(new MapFunction<String, String>() {

            @Override
            public String map(String s) throws Exception {
                    return s.toString();
            }
        });
        FlatMapOperator<String, Tuple2<String, Integer>> a = mapMain.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {


                String[] words = s.split(" ");
                //System.out.println(words[0]);
                if(!CacheManager.isContainKey(words[0])) { //未命中s
                    CacheManager.putCache(s,new Cache());
                    signal = "main";
                    /**
                     * 收到元组b的时间
                     */
                    Date date = new Date();
                    SimpleDateFormat sdfc = new SimpleDateFormat("HH:mm:ss:SSS");

                    if(words[0].equals("error")){
                        //CacheManager.clearOnly(words[0]);
                        signal="sub-main";
                       // System.out.print("主算子无法接受数据，由备份算子处理");
                        //TODO 让备份算子处理5号元组
                        String zifuchuan = s.split(" ")[1];
                        SimpleDateFormat okTime = new SimpleDateFormat("YYYY-MM-dd:HH:mm:ss:SSS");
                         correctTime = okTime.format(new Date());

                        System.out.println("备份算子处理故障点：Tuple ID"+zifuchuan+"故障发生时间为："+ErrorTime+"故障解决时间为"+correctTime+" ");

                    }

                    for (String word: words){
                        //System.out.println(word);
                        collector.collect(new Tuple2<String,Integer>(word,1));
                    }

                } else{
                    CacheManager.clearOnly(s);
                    //System.out.println("命中");
                }
                //traverse all word, then wrap as tuple return

            }
        });

        //b为a的备份,a与b同时接受上游传输的数据
        FlatMapOperator<String, Tuple2<String, Integer>> b = mapSubMain.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                //split word by whitespace
                //System.out.println(s);
                String[] words = s.split(" ");
                if(CacheManager.isContainKey(words[0])) { //命中s
                    CacheManager.clearOnly(s);
                    //System.out.println("命中");
                }
                else{
                    signal = "sub-main";
                    CacheManager.putCache(s,new Cache());
                    /**
                     * 收到备份算子的时间
                     */
                    for (String word: words){
                        //System.out.println(word);
                        collector.collect(new Tuple2<String,Integer>(word,1));
                    }
                }
            }
        });
        DataSet resultSet1 = a.groupBy(0).sum(1);
        DataSet resultSet2 = b.groupBy(0).sum(1);

          if(signal.equals( "main")) {
                  resultSet1.print();
          }else
              if(signal.equals("sub-main")){
                  resultSet2.print();
              }
    }


}
