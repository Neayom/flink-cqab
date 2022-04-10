package com.dlnu.Exception;

import com.dlnu.Cache.Cache;
import com.dlnu.Cache.CacheManager;
import com.dlnu.Entity.CQABEntity;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * Created by lgx on 2022/3/21.
 */
public class DataLossException {
    static String ErrorTime;//故障发生的时间
    static String correctTime;//故障解决的时间

    public static String DataLostException() {
        CacheManager.putCache("error",new Cache(CQABEntity.getErrorPoint()));
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYY-MM-dd:HH:mm:ss:SSS");
        ErrorTime = simpleDateFormat.format(new Date());
        System.err.println("Data Lose Exception:主算子处理+ErrorPoint过程中出现异常交由备份算子处理");
        //模拟主算子接受不到该数据,发送故障信号
        return "error "+CQABEntity.getErrorPoint();
    }
}
