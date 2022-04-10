package com.dlnu.Function;

import com.dlnu.BackUp.BackUpProcess;
import com.dlnu.Cache.CacheManager;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by lgx on 2022/4/9.
 */
public class CQABFlatMapFunction implements FlatMapFunction<String, String> {
    static String correctTime;//故障解决的时间
    static String ErrorTime;//故障发生的时间
    public void flatMap(String value, Collector<String> out) throws Exception {
        String[] tokens = value.split(" ");
        //out.collect(value+"mark");
        for (String token: tokens) {
            if (token.equals("error")) {
                SimpleDateFormat ErrTime = new SimpleDateFormat("YYYY-MM-dd:HH:mm:ss:SSS");
                ErrorTime = ErrTime.format(new Date());
                String Point = CacheManager.getCacheInfo("error").getKey();
                BackUpProcess.process(Point);
                SimpleDateFormat okTime = new SimpleDateFormat("YYYY-MM-dd:HH:mm:ss:SSS");
                correctTime = okTime.format(new Date());
                System.out.println("备份算子处理故障点：Tuple ID " + Point + " 故障发生时间为：" + ErrorTime + "故障解决时间为" + correctTime + " ");

            }else{
                System.out.println(""+value);
                out.collect(value+" mark");
            }
        }
    }
}
