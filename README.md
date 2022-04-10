Flink-CQAB
Flink新的容错算法
打包后：--inputPath /root/file-for-xhtflink/flink/hello.txt --outPath /root/file-for-xhtflink/flink/r2 --ErrorPoint 7

/**
参数有三个
 inputPath：输入数据集地址  outPath：数据清洗后的文件地址 ErrorPoint：模拟的故障点，为输入数据集的id
该任务运行完成后，会生成两个文件，一个是outPath，是数据清洗后的结果，第二个文件是outPath1，该文件为故障处理信息

**/
需要注意的点是：outPath路径当前系统中不能存在
/**

