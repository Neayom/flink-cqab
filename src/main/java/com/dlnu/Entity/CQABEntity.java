package com.dlnu.Entity;

/**
 * Created by lgx on 2022/4/9.
 */
public class CQABEntity {
    public static String ErrorPoint;
    public static String inputPath;
    public static String outPath;

    public static String getErrorPoint() {
        return ErrorPoint;
    }

    public static void setErrorPoint(String errorPoint) {
        ErrorPoint = errorPoint;
    }

    public static String getInputPath() {
        return inputPath;
    }

    public static void setInputPath(String inputPath) {
        CQABEntity.inputPath = inputPath;
    }

    public static String getOutPath() {
        return outPath;
    }

    public static void setOutPath(String outPath) {
        CQABEntity.outPath = outPath;
    }
}
