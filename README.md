# user_data_analys

```bash
.
├── README.md
├── demo #编译成的jar包
│   ├── DailyFundFlow.jar
│   ├── InterestRateImpact.jar
│   ├── UserActivity.jar
│   ├── WeeklyFundFlow.jar
│   ├── com
│   │   └── example
│   └── src #源代码
│       ├── main
│           └── java
│               └── com
│                   └── example
│                       ├── App.java
│                       ├── DailyFundFlow.java
│                       ├── InterestRateImpact.java
│                       ├── UserActivity.java
│                       └── WeeklyFundFlow.java
├── output #输出（txt文件）
├── run.sh #运行
├── pom.xml
└── target #编译目录

```
\
输出结果详见output

# 任务1：


**一、设计思路**

1. **Mapper**：

    * 读取user_balance_table中的每一行。
    * 解析日期、total_purchase_amt和total_redeem_amt。
    * 处理缺失值，将其视为零。
    * 将日期作为键，total_purchase_amt和total_redeem_amt的组合作为值输出。
2. **Reducer**：

    * 聚合每个日期的资金流入和流出量。输出日期和汇总的资金流入和流出量。

**二、运行结果**

给定一个包含交易数据的文件作为输入，程序会统计每天的资金流入和流出总额，并将结果输出到指定的文件中。输出文件的每一行格式为 “交易日期，总资金流入，总资金流出”。



# 任务2：



**一、设计思路**

1. **Mapper**：

    * 读取每日资金流入流出统计的结果。
    * 解析日期和资金流入流出量。
    * 将日期转换为星期几。
    * 将星期几作为键，资金流入流出量作为值输出。
2. **Reducer**：

    * 聚合每个星期几的资金流入和流出量。
    * 计算每个星期几的平均资金流入和流出量。
    * 输出星期几和平均资金流入流出量。
3. **主函数**：

    * 设置 Hadoop 作业的配置信息，指定作业名称、输入输出路径、Mapper 和 Reducer 类等。最后等待作业完成并根据结果退出程序。

**二、运行结果**

给定一个包含日期和资金流入流出数据的文件作为输入，程序会统计每个星期几的平均资金流入和平均资金流出，并将结果输出到指定的文件中。输出文件的每一行格式为 “星期几，平均资金流入，平均资金流出”。


# 任务3：



## UserActivityAnalysis  

**一、设计思路**

1. **Mapper 阶段**：

    * 读取user_balance_table中的每一行。
    * 解析用户ID、direct_purchase_amt和total_redeem_amt。
    * 如果用户当天有直接购买或赎回行为，则将用户ID作为键，值为1输出
2. **Reducer 阶段**：

    * 聚合每个用户的活跃天数。
    * 输出用户ID和活跃天数。
3. **主函数**：

    * 设置 Hadoop 作业的配置信息，指定作业名称、输入输出路径、Mapper 和 Reducer 类等。最后等待作业完成并根据结果退出程序。

**二、运行结果**

给定一个包含用户活动数据的文件作为输入，程序会统计每个用户的活跃天数，并将结果输出到指定的文件中。输出文件的每一行格式为 “用户 ID，活跃天数”。


## SortByActiveDays

1. **Mapper 阶段**：
   - 对于非空行，使用制表符（`\t`）分割输入行，期望得到两部分内容，分别对应用户 ID 和活跃天数。如果分割后长度不为 2，则打印无效输入行日志并忽略该行。
   - 如果格式正确，将用户 ID 和活跃天数交换，输出为 `<IntWritable, Text>`，其中 `IntWritable` 是活跃天数，`Text` 是用户 ID。



 **运行结果**:
   - 对输入文件中的用户 ID 和活跃天数进行处理，按照活跃天数进行降序排列，最终输出为每个用户 ID 及其对应的活跃天数，其中活跃天数高的用户排在前面。


# 任务4分析银行利率对申购/赎回行为的影响：



**map阶段：**

1. 在setup方法中读取利率文件，并将日期和利率存储在内存中。
2. 读取user_balance_table中的每一行。
3. 解析日期、total_purchase_amt和total_redeem_amt。
4. 根据日期查找对应的利率区间。
5. 将利率区间作为键，资金流入流出量作为值输出。

**reduce阶段：**

1. 聚合每个利率区间的资金流入和流出量
2. 计算每个利率区间的平均资金流入和流出量。
3. 输出利率区间和平均资金流入流出量。

### 结论：

从结果中可以看出，随着利率的增加，资金的流入和流出量也在增加。这表明较高的利率可能会吸引更多的资金流入，同时也会导致更多的资金流出。

有不到一半的时间里，单日年化收益高于周平均年化收益。这启发我们要减少非必要的调仓操作，长期持有的收益概率高于追涨杀跌。

‍





