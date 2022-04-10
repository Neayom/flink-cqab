package com.dlnu.Service;

import com.dlnu.Entity.CQABEntity;
import com.dlnu.Function.CQABFlatMapFunction;
import com.dlnu.Function.CQABMapFunction;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

/**
 * Created by xuwei.tech on 2018/10/8.
 */
public class BatchWordCountJava {


    public static void main(String[] args) throws Exception{
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String inputPath = parameterTool.get("inputPath");
        String outPath = parameterTool.get("outPath");
        //inputPath = "src/main/resources/hello.properties";
        //outPath = "src/main/resources/hello101";
        String ErrorPoint = parameterTool.get("ErrorPoint");
        //String inputPath = "src/main/resources/hello.properties";
        //String outPath = "hello2.txt";
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取文件中的内容
        DataStream<String> text = env.readTextFile(inputPath).setParallelism(1);
        DataStream<String> map = text.map(new CQABMapFunction());
        DataStream<String> result = map.flatMap(new CQABFlatMapFunction());
        //counts.print();
        result.writeAsText(outPath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        env.execute("batch word count");
    }
}