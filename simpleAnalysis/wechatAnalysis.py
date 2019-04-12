# -*- coding: utf-8 -*-
"""
1. 将聊天数据按照群号，划分不同的文件 √
2. 统计每个群中发言次数最多的ID,发言长短,平均长度,统计用户发链接或图片的次数(也可以统计发图片链接的次数/总发言数), @次数,被@次数。√
3. 统计每个群的活跃度，和所有群加起来的活跃度，在每天时间上的分布（得出的按照每分钟的效果并不好看，尝试每小时或每半小时！）
4. 统计每个群中，被@的次数
5. 统计四个群中被@的次数，同5
6. 分词，并从腾讯词库中提取词向量。
"""
from commonTools import wechatContent, ConnectDatabase as cd
import threading
import time
import os
import jieba
import pymysql


def get_stopwords():
    """
    加载停用词
    :return: 停用词list
    """
    # 获取当前路径
    current_path = os.path.abspath(os.path.dirname(os.getcwd()))
    filepath = current_path + "/data/stopwords.txt"
    stopwords = [line.strip() for line in open(filepath, 'r', encoding='utf-8').readlines()]
    return stopwords


def split_content(sender_conn, talker_content):
    """
    将数据按照内容拆分，拆分的表为wechat_sender 包含了发言次数、发言长度及其他发言规律。
    :param sender_conn: wechat_sender 表数据库连接
    :param talker_content: 每条信息的id、type、群号、content
    :return:
    """

    # 接受单条消息的相关信息
    msgId = talker_content[0]
    message_type = talker_content[1]
    chatroomNum = talker_content[2]
    content = talker_content[3]
    wechat = wechatContent.WechatContent(content)
    result = wechat.splitContent()
    user_id = result["user_id"]
    # 如果解析失败，则为特殊信息，不需要考虑
    if user_id is "_":
        return None

    # 检查是否有数据库记录
    situation = "WHERE chatroom = '%s' AND username = '%s'" % (chatroomNum, user_id)
    title_list = ["*"]
    sender_cursor = sender_conn.select_order(title_list, situation)
    # res = (countId,chatroomNum,user_id, sendTimes, allLength, imgTimes, urlTimes, atTimes)
    res = sender_cursor.fetchone()

    # 如果没有记录，创建数据
    if res is None:
        # 没有记录,创建记录
        new_data = {"chatroom": chatroomNum, "username": user_id, "sendTimes": str(0),
                    "allLength": str(0), "atTimes": str(0),
                    "imgTimes": str(0), "urlTimes": str(0)}
        # 新建记录，插入数据，将主键设置为空是为了避免主键的数据库检查
        sender_conn.insertData(new_data, primary_key="")
        res = (0, chatroomNum, user_id, 0, 0, 0, 0, 0)

    # 按照信息类型，统计数据，并更新数据表
    # 信息类型为文字
    if message_type is "1":
        context = result["only_con"]
        at_list = wechat.splitAt()
        update_data = {"sendTimes": str(int(res[3]) + 1), "allLength": str(int(res[4]) + len(context)),
                       "atTimes": str(int(res[7]) + len(at_list))}
        sender_conn.update_database(update_data, situation)

    # 信息类型为图片
    elif message_type is "3":
        update_data = {"sendTimes": str(int(res[3]) + 1), "imgTimes": str(int(res[5]) + 1)}
        sender_conn.update_database(update_data, situation)

    # 信息类型为其他类型
    else:
        update_data = {"sendTimes": str(int(res[3]) + 1), "urlTimes": str(int(res[6]) + 1)}
        sender_conn.update_database(update_data, situation)

    # 显示处理进度
    print("***第%s条已处理***" % msgId)


def cut_word(word_conn, message_info):

    # 加载停用词辞典(这里为了不影响语意，暂时不删除停用词和标点)
    # stopwords = get_stopwords()
    msgId = message_info[0]
    msgType = message_info[1]
    content = message_info[3]
    if msgType is not "1":
        return None

    wechat = wechatContent.WechatContent(content)
    result = wechat.splitWithAt()
    # 去掉user_id和@的纯内容
    context = result["only_con"]

    # 开始分词
    seg = jieba.cut(context)
    new_data = {"msgId": str(msgId), "context": context, "jieba_word": "/".join(seg), "atList": "/".join(result["atList"])}
    # 将数据插入数据库
    word_conn.insertData(new_data)
    # 保存数据
    # conn.save(data)
    # print(word_list, atList)
    # time.sleep(1000)
    print("***第%s条已处理***" % msgId)


def multi_run(coreNum, targetTable, targetFunction):
    """
    多线程启动函数
    :param targetFunction: 多线程调用的函数
    :param targetTable: 要存储的表名
    :param coreNum: 线程数
    :return:
    """
    # 与message表建立数据库连接
    message_conn = cd.MySQLCommand()
    message_conn.connectMysql(table="wechat_message")
    message_cursor = message_conn.select_order(["msgId", "type", "talker", "content"])

    conn_dict = {}
    for j in range(coreNum):
        multi_conn = cd.MySQLCommand()
        multi_conn.connectMysql(table=targetTable)
        conn_dict["conn_%s" % str(j)] = multi_conn

    # 设计一个钩子
    TAG = True
    while TAG:
        # 多线程解析content
        for multi_conn in conn_dict.values():
            message = message_cursor.fetchone()
            # 如果已经遍历结束，直接结束
            if message is None:
                # message_conn.closeMysql()
                TAG = False
                break
            th = threading.Thread(target=targetFunction, args=(multi_conn, message, ))
            # print("第", i, "个线程开启")
            th.start()
            th.join()
    # 关闭连接
    for conn_j in conn_dict.values():
        conn_j.closeMysql()
    message_conn.closeMysql()


def count_sender_by_chatroom(coreNum):
    """
    统计发言规律
    :param coreNum:
    :return:
    """
    multi_run(coreNum, "wechat_sender", split_content)


def cut_word_jieba(coreNum):
    """
    进行jieba分词，并获得@list
    :param coreNum:
    :return:
    """
    multi_run(coreNum, "wechat_word", cut_word)


def get_word_vector(vector_conn, new_conn, words, ignore_word):

    # 根据腾讯词库获取词向量
    # 因为词库的词量较为丰富，暂时忽略停用词，遇到没有的词汇，保存成txt，或者考虑自己训练。

    word_list = str(words).replace("\n", "").split("/")
    for word in word_list:
        situation = "where word = '%s'" % word
        try:
            vector_cursor = vector_conn.select_order(["*"], situation)
        except pymysql.err.ProgrammingError:
            continue
        word_vector = vector_cursor.fetchone()
        if word_vector is None:
            # ignore_word.add(word)
            word_data = {"word": word, "vector": str(0)}

            new_conn.insertData(word_data, primary_key="word")
        """
        else:
            vec = str(word_vector[1:]).replace("(", "").replace(")", "")
            word_data = {"word": word, "vector": vec}

            new_conn.insertData(word_data, primary_key="word")
            # print("%s 已导入成功！" % word)
        """


def speed_word_vector(coreNum):
    word_conn = cd.MySQLCommand()
    word_conn.connectMysql(table="wechat_word")
    situation = "where msgId > %s" % str(90909)
    word_cursor = word_conn.select_order(["msgId", "jieba_word"], situation=situation)
    ignore_word = set()
    conn_dict = {}
    # 建立数据库连接

    for i in range(coreNum):
        vector_conn = cd.MySQLCommand()
        vector_conn.db = "tencent_word_vec"
        vector_conn.connectMysql(table="tc_word_vec")
        new_conn = cd.MySQLCommand()
        new_conn.connectMysql(table="wechat_vector")
        conn_dict["conn_%s" % str(i)] = (vector_conn, new_conn)
    TAG = True
    while TAG:
        ts = []
        for conn_tuple in conn_dict.values():
            try:
                (msgId, words) = word_cursor.fetchone()
            except TypeError:
                TAG = False
                break
            # print(words)
            th = threading.Thread(target=get_word_vector, args=(conn_tuple[0], conn_tuple[1], words, ignore_word,))
            th.start()
            print("第%s条信息开始处理！" % str(msgId))
            ts.append(th)
        for th in ts:
            th.join()

    with open("data/ignore_word.txt", "w", encoding="utf-8") as f:
        for word in ignore_word:
            f.write(str(word) + "\n")

    for conn_tuple in conn_dict.values():
        conn_tuple[0].closeMysql()
        conn_tuple[1].closeMysql()
    word_conn.closeMysql()


def main():
    startTime = time.time()
    coreNum = int(input("线程数量："))
    # 进行简单的统计分析
    # count_sender_by_chatroom(int(coreNum))
    # 分词
    cut_word_jieba(coreNum)
    # 提取词向量
    get_word_vector()
    endTime = time.time()
    print('运行时间：%.3f' % (endTime - startTime))


def clear_wechat_message():
    """
    # 清除数据库中发言数量少于20的聊天记录。
    :return:
    """
    message_conn = cd.MySQLCommand()
    message_conn.connectMysql(table="wechat_message")
    for chatroom in message_conn.select_distinct():
        chatroom = chatroom[0]
        res = message_conn.cursor.execute("select talker from wechat_message where talker = '%s'" % chatroom)
        print("*****", res)
        if res < 20:
            message_conn.cursor.execute("delete from wechat_message where talker = '%s'" % chatroom)
            message_conn.cursor.execute("delete from wechat_sender where chatroom = '%s'" % chatroom)
            message_conn.conn.commit()
            message_conn.cursor.execute("select nickname from wechat_contact where username = '%s'" % chatroom)
            print(message_conn.cursor.fetchone()[0], "*****已删除")
    message_conn.closeMysql()


if __name__ == '__main__':
    # 开启并行分词
    # jieba.enable_parallel(4)

    # 清理数据库信息
    # clear_wechat_message()
    # get_stopwords()
    # main()
    # get_word_vector()
    speed_word_vector(32)
