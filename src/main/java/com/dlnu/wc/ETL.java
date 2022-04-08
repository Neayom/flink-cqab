package com.dlnu.wc;

import com.dlnu.Cache.Cache;
import com.dlnu.Cache.CacheManager;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ETL {
    //参数1 故障点 参数2 输入
    static boolean isError = false;
    static String ErrorPoint ; //模拟故障的点
    static String ErrorTime;//故障发生的时间
    static String correctTime;//故障解决的时间
    static String inputPath;
    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        inputPath = parameterTool.get("inputPath");
        /*ErrorPoint = parameterTool.get("ErrorPoint");*/
       //inputPath = "src/main/resources/hello.txt";
       ErrorPoint = "9";

        // create execution env
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        //read data from file
        //it is extending DataSet
        //DataSet<String> stringDataSource = executionEnvironment.readTextFile(inputPath);
        DataSource<String> inputDataSet = executionEnvironment.readTextFile(inputPath);
        MapOperator<String, String> mapMain = inputDataSet.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                //将元组ID缓存到缓存队列中...
                CacheManager.putCache(s.split(" ")[0],new Cache(s.split(" ")[0]));
                if (s.split(" ")[0].equals(ErrorPoint)) {
                    //主算子接收不到ErrorPoint，发生数据丢失异常...将异常信息发送给下游主算子
                    return DataLostException();
                } else {
                    return s.toString();
                }
            }
        });
        FlatMapOperator<String, String> stringStringFlatMapOperator = mapMain.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] words = s.split(" ");
                //System.out.println(words[0]);
                    //CacheManager.putCache(words[0], new Cache());
                //当主算子接受到异常信息后，process
                    if (words[0].equals("error")) {
                        //CacheManager.clearOnly(words[0]);
                        // 让备份算子处理5号元组
                        String Point = CacheManager.getCacheInfo("error").getKey();
                        //System.out.println(Point); 得到发生故障的offset
                        process(Point);
                        SimpleDateFormat okTime = new SimpleDateFormat("YYYY-MM-dd:HH:mm:ss:SSS");
                        correctTime = okTime.format(new Date());
                        System.out.println("备份算子处理故障点：Tuple ID " + Point + " 故障发生时间为：" + ErrorTime + "故障解决时间为" + correctTime + " ");

                    } else {
                        collector.collect(s + " mark");
                    }
                //traverse all word, then wrap as tuple return
            }
        });
        stringStringFlatMapOperator.print();
    }
    public static String DataLostException() {
        CacheManager.putCache("error",new Cache(ErrorPoint));
        isError = true;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYY-MM-dd:HH:mm:ss:SSS");
        ErrorTime = simpleDateFormat.format(new Date());
        System.out.println("主算子处理"+ErrorPoint+"过程中出现异常交由备份算子处理");
        //模拟主算子接受不到该数据,发送故障信号
        return "error "+ErrorPoint;
    }
    //备份算子处理过程
    public static void  process(String target) throws Exception {
        if (CacheManager.isContainKey(target)) {
            CacheManager.putCache(target,new Cache(target));
            ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
            DataSource<String> stringDataSource = environment.readTextFile(inputPath);
            FlatMapOperator<String, String> stringStringFlatMapOperator = stringDataSource.flatMap(new FlatMapFunction<String, String>() {
                @Override
                public void flatMap(String s, Collector<String> collector) throws Exception {
                    if (s.split(" ")[0].equals(target)) {
                        collector.collect(s + " mark");
                    }
                }
            });
            stringStringFlatMapOperator.print();
        }
    }


}
