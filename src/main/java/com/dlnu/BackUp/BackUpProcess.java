package com.dlnu.BackUp;

import com.dlnu.Cache.Cache;
import com.dlnu.Cache.CacheManager;
import com.dlnu.Entity.CQABEntity;
import com.dlnu.Environment.executionEnvironment;
import com.dlnu.Service.BatchWordCountJava;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;

import java.io.FileWriter;


/**
 * Created by lgx on 2022/4/9.
 */
public class BackUpProcess {
    public static void  process(String target) throws Exception {
            CacheManager.putCache(target,new Cache(target));
            //ErrorData发生错误时的数据信息
            //String ErrorData  = executionEnvironment.getEnvironment(target);
            String ErrorData = "9";
            if(ErrorData.equals("Null ErrorPoint")){
                System.out.println("输入的ErrorPoint不存在！");
                Logger logger = LoggerFactory.getLogger(BackUpProcess.class);
                logger.warn("输入的ErrorPoint不存在！");
            }else{
                /*String outPath = CQABEntity.getOutPath();
                System.out.println(outPath+"12345");*/
                BufferedWriter br = new BufferedWriter(new FileWriter("/root/flink-1.12.7/ZS11"+"1"));
                System.out.println(ErrorData);
                br.write(ErrorData+" mark");
                br.flush();
                //br.close();
            }

    }
}
