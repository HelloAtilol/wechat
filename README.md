# WeChat

## 相关数据库及文件初始化； 
1. python环境(python 3+)，使用到的包：
    - pymysql
    - csv
    - re
    - time
    - threading
    - os
    - jieba

2. 数据库模型如图：需要创建以下四个数据库，保存数据，优化处理过程：

![数据库模型](C:\Users\Administrator\Desktop\数据库模型.PNG)

3. 初始化数据库链接  
相关参数在***/commonTools/ConnectDatabese.py***文件。   
    ```python
    def __init__(self):
            # 数据库地址,如果是本地数据库，地址为localhost
            self.host = '192.168.1.107'
            # 端口号
            self.port = 3306
            # 用户名
            self.user = 'root'
            # 密码，数据库密码
            self.password = 'sim509'
            # 数据库名
            self.db = 'wechat_message'
    ```

## 1. 将sqlite数据库中的数据导入到mysql数据库；
主要有两个数据库(message, rcontact), 操作顺序：
1. 利用sqlcipher将两个表分别保存为csv表格，然后利用notepad++将表格转化为utf-8编码，不然会在程序执行时出现问题；
2. 按照数据库的结构，创建mysql的表格，message对应的是聊天记录，rcontact记录的是群里的用户ID及昵称；
3. 执行ConnectDatabase.py, 将数据导入到数据库，两个表格只需要修改文件名称即可，即修改下方文件名；  
    ```python
    # 读取csv文件
     with open('data/wechat_contact_0314.csv', 'r', encoding="UTF-8") as f:
    ```

>TODO: 使用多线程保存数据，目前单线程效率较低，51w数据需要的时间较长。

## 2. 将数据库的数据按照群号拆分；(如果不需要，可以不做)

1. 按照群号将数据库导出为txt的版本;  
2. 使用多线程提升效率;  

	上述操作只需要执行***/simpleAnalysis/contentByChatroom.py***文件即可，目前存在一个问题，就是会有一些不需要的群信息也会被提取出来，建议手动去除这些不必要的信息。

## 3. 完成之前统计性描述的部分；

1. 将聊天数据按照群号，划分不同的文件 √
2. 统计每个群中发言次数最多的ID,发言长短,平均长度,统计用户发链接或图片的次数(也可以统计发图片链接的次数/总发言数), @次数。√  

	上述操作与第四步在一起，只需要执行一遍***/simpleAnalysis/simpleAnalysis.py***即可。

> TODO: 统计每个群的活跃度，和所有群加起来的活跃度，在每天时间上的分布（得出的按照每分钟的效果并不好看，尝试每小时或每半小时！）;暂时不考虑

## 4. other
1. 使用结巴分词，并将@的list保存在数据库中;在Linux环境中，可以开启多进程分词，windows无法使用该功能；
    ```python
    # 开启并行分词,并行数量取决于CPU核心数量和线程数量；
    jieba.enable_parallel(4)
    ```
    上述操作只需要执行***/simpleAnalysis/simpleAnalysis.py***即可，需要输入的线程数量可以根据计算机性能判断，建议8-32。目前只能做到分词，提取词向量的操作正在优化。

> TODO:
> 1. 获取分词的词向量，在此之前要先确定停用词辞典； 
> 因为标点符号在都有自己的词向量，所以暂时不需要考虑消除停用词，此外，关于词向量降维，目前暂时不需要。
> 2. 主题词抽取，对话流分解;
