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
       /* inputPath = parameterTool.get("inputPath");
        ErrorPoint = parameterTool.get("ErrorPoint");*/
       inputPath = "src/main/resources/hello.txt";
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
                if (s.split(" ")[0].equals(ErrorPoint)) {
                    isError = true;
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYY-MM-dd:HH:mm:ss:SSS");
                    ErrorTime = simpleDateFormat.format(new Date());
                    System.out.println("主算子处理"+ErrorPoint+"过程中出现异常交由备份算子处理");
                    //模拟主算子接受不到该数据,发送故障信号
                    return "error "+s;
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
                if (!CacheManager.isContainKey(words[0])) { //未命中s
                    CacheManager.putCache(words[0], new Cache());
                    if (words[0].equals("error")) {
                        //CacheManager.clearOnly(words[0]);
                        // 让备份算子处理5号元组
                        String zifuchuan = s.split(" ")[1];
                        process(zifuchuan);
                        SimpleDateFormat okTime = new SimpleDateFormat("YYYY-MM-dd:HH:mm:ss:SSS");
                        correctTime = okTime.format(new Date());
                        System.out.println("备份算子处理故障点：Tuple ID" + zifuchuan + "故障发生时间为：" + ErrorTime + "故障解决时间为" + correctTime + " ");

                    } else {
                        collector.collect(s + " mark");
                    }
                } else {
                    CacheManager.clearOnly(words[0]);
                    //System.out.println("命中");
                }
                //traverse all word, then wrap as tuple return
            }
        });
        stringStringFlatMapOperator.print();

    }
    //备份算子处理过程
    public static void  process(String target) throws Exception {
        if (!CacheManager.isContainKey(target)) {
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
