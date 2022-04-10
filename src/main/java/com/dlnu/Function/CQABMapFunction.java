package com.dlnu.Function;


import com.dlnu.Cache.Cache;
import com.dlnu.Cache.CacheManager;
import com.dlnu.Entity.CQABEntity;
import org.apache.flink.api.common.functions.MapFunction;

import static com.dlnu.Exception.DataLossException.DataLostException;


/**
 * Created by lgx on 2022/4/9.
 */
public class CQABMapFunction implements MapFunction<String,String>{

    @Override
    public String map(String s) throws Exception {
        String ErrorPoint = CQABEntity.getErrorPoint(); //模拟故障的点
        System.out.println(ErrorPoint+"测试测试测试测试");
        //将元组ID缓存到缓存队列中...
        CacheManager.putCache(s.split(" ")[0],new Cache(s.split(" ")[0]));
        if (s.split(" ")[0].equals("9")) {
            //主算子接收不到ErrorPoint，发生数据丢失异常...将异常信息发送给下游主算子
            return DataLostException();
        } else {
            return s.toString();
        }
    }
}
