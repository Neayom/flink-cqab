package com.dlnu.Service;

import com.dlnu.BackUp.BackUpProcess;
import com.dlnu.Cache.Cache;
import com.dlnu.Cache.CacheManager;
import com.dlnu.Entity.CQABEntity;
import com.dlnu.Environment.executionEnvironment;
import com.dlnu.Function.CQABFlatMapFunction;
import com.dlnu.Function.CQABMapFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

import static com.dlnu.Exception.DataLossException.DataLostException;

/**
 * Created by xuwei.tech on 2018/10/8.
 */
public class BatchWordCountJava1 {


    public static void main(String[] args) throws Exception{
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String inputPath = parameterTool.get("inputPath");
        String outPath = parameterTool.get("outPath");
        //String inputPath = "src/main/resources/hello.properties";
        //String outPath = "src/main/resources/neayom";
        String ErrorPoint = parameterTool.get("ErrorPoint");
        //String ErrorPoint="9";
        //String inputPath = "src/main/resources/hello.properties";
        //String outPath = "hello2.txt";
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取文件中的内容
        DataStream<String> text = env.readTextFile(inputPath).setParallelism(1);
        DataStream<String> map = text.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                //String ErrorPoint = CQABEntity.getErrorPoint(); //模拟故障的点
                //将元组ID缓存到缓存队列中...
                CacheManager.putCache(s.split(" ")[0],new Cache(s.split(" ")[0]));
                if (s.split(" ")[0].equals(ErrorPoint)) {
                    //主算子接收不到ErrorPoint，发生数据丢失异常...将异常信息发送给下游主算子
                    CacheManager.putCache("error",new Cache(ErrorPoint));
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYY-MM-dd:HH:mm:ss:SSS");
                    String ErrorTime = simpleDateFormat.format(new Date());
                    System.err.println("Data Lose Exception:主算子处理+ErrorPoint过程中出现异常交由备份算子处理");
                    //模拟主算子接受不到该数据,发送故障信号
                    return "error "+ErrorPoint;
                } else {
                    return s.toString();
                }
            }
        });
        DataStream<String> result = map.flatMap(new FlatMapFunction<String, String>() {
             String correctTime;//故障解决的时间
             String ErrorTime;//故障发生的时间
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] tokens = value.split(" ");
                //out.collect(value+"mark");

                    if (tokens[0].equals("error")) {
                        SimpleDateFormat ErrTime = new SimpleDateFormat("YYYY-MM-dd:HH:mm:ss:SSS");
                        ErrorTime = ErrTime.format(new Date());
                        String Point = CacheManager.getCacheInfo("error").getKey();
                        /**
                         *
                         */
                        CacheManager.putCache(Point,new Cache(Point));
                        //ErrorData发生错误时的数据信息
                        //String ErrorData  = executionEnvironment.getEnvironment(target);
                        //++++++++++++++++++++++++++++++++++++++
                        String ErrorData = "";
                        File file = new File(inputPath);
                        try {
                            BufferedReader br = new BufferedReader(new FileReader(file));
                            String s = null;

                            while((s = br.readLine())!=null){
                                //使用readLine方法，一次读一行
                                if(s.split(" ")[0].equals(Point)){
                                    ErrorData = s;
                                    System.out.println("----=="+ErrorData);
                                    break;
                                }else{
                                    ErrorData = "Null ErrorPoint";
                                }
                                // br.close();
                            }
                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        }
                        //++++++++++++++++++++++++++++++++++++++
                        //String ErrorData = executionEnvironment.getEnvironment(Point);
                        if(ErrorData.equals("Null ErrorPoint")){
                            System.out.println("输入的ErrorPoint不存在！");
                            Logger logger = LoggerFactory.getLogger(BackUpProcess.class);
                            logger.warn("输入的ErrorPoint不存在！");
                        }else{
                            //String outPath = CQABEntity.getOutPath();
                            System.out.println(outPath+"12345");
                            BufferedWriter br = new BufferedWriter(new FileWriter(outPath+"1"));
                            System.out.println(ErrorData);
                            br.write(ErrorData+" mark");
                            br.flush();
                            //br.close();
                        }
                        /**

                         */
                        SimpleDateFormat okTime = new SimpleDateFormat("YYYY-MM-dd:HH:mm:ss:SSS");
                        correctTime = okTime.format(new Date());
                        System.out.println("备份算子处理故障点：Tuple ID " + Point + " 故障发生时间为：" + ErrorTime + "故障解决时间为" + correctTime + " ");

                    }else{
                        System.out.println(""+value);
                        out.collect(value+" mark");
                    }
                }

        });
        //counts.print();
        result.writeAsText(outPath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        env.execute("batch word count");
    }
}