# WeChat
目前微信的数据量是23个群，51W的群聊记录。主要是神经外科的相关
疾病，涉及到的医生与医院大多为北京天坛医院有关。


## 1. 将sqlite数据库中的数据导入到mysql数据库；
主要有两个数据库(message, rcontact), 操作顺序：
1. 利用sqlcipher将两个表分别保存为csv表格，然后利用notepad++将表格转化为utf-8编码，不然会在程序执行时出现问题；
2. 按照数据库的结构，创建mysql的表格，message对应的是聊天记录，rcontact记录的是群里的用户ID及昵称；
3. 执行ConnectDatabase.py, 将数据导入到数据库，两个表格只需要修改文件名称即可；    

>TODO: 使用多线程保存数据，目前单线程效率较低，51w数据需要的时间较长。

## 2. 将数据库的数据按照群号拆分；

>TODO:
> 1. 按照群号将数据库导出为txt的版本；  
> 2. 使用多线程提升效率； 

## 3. 完成之前统计性描述的部分；

>TODO:
> 1. 将聊天数据按照群号，划分不同的文件 
> 2. 统计每个群中发言次数最多的ID 
> 3. 统计每个群的活跃度，和所有群加起来的活跃度，在每天时间上的分布（得出的按照每分钟的效果并不好看，尝试每小时或每半小时！）
> 4. 统计每个人的发言长短，和平均长度 
> 5. 统计每个群中，被@的次数 
> 6. 统计四个群中被@的次数，同5
> 7. 统计用户发链接或图片的次数(也可以统计发图片链接的次数/总发言数)
> 8. 四个群分别提到最多的词(去掉stopwords) 

## 4. other
