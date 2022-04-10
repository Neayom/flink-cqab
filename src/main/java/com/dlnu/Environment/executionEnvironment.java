package com.dlnu.Environment;

import com.dlnu.Entity.CQABEntity;
import com.dlnu.Service.BatchWordCountJava;

import java.io.*;

/**
 * Created by lgx on 2022/4/9.
 */
public class executionEnvironment {
    public static String getEnvironment(String errorPoint) throws IOException {
        String inputPath = CQABEntity.getInputPath();
        String target;
        File file = new File(inputPath);
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String s = null;

            while((s = br.readLine())!=null){
                //使用readLine方法，一次读一行
                if(s.split(" ")[0].equals(errorPoint)){
                    target = s;
                    System.out.println("----=="+target);
                    return target;
                }
               // br.close();
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return "Null ErrorPoint";
    }
}
