# -*- coding: utf-8 -*-
"""
1. 将聊天数据按照群号，划分不同的文件 √
2. 统计每个群中发言次数最多的ID,发言长短,平均长度,统计用户发链接或图片的次数(也可以统计发图片链接的次数/总发言数), @次数,被@次数。√
3. 统计每个群的活跃度，和所有群加起来的活跃度，在每天时间上的分布（得出的按照每分钟的效果并不好看，尝试每小时或每半小时！）
4. 统计每个群中，被@的次数
5. 统计四个群中被@的次数，同5
6. 分词，并从腾讯词库中提取词向量。
TODO: 存在一个BUG，就是在多线程开启，建立多个数据库连接时，关闭全部数据库连接会报错，只有少关闭一个message_conn才能避免错误，目前错误原因未知。
"""
from commonTools import wechatContent, ConnectDatabase as cd
import threading
import time


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


def count_sender_by_chatroom(coreNum):
    """
    按照群和用户，拆分每个用户的发言规律
    :param coreNum: 线程数
    :return:
    """

    # 与message表建立数据库连接
    message_conn = cd.MySQLCommand()
    message_conn.connectMysql(table="wechat_message")
    message_cursor = message_conn.select_order(["msgId", "type", "talker", "content"])

    conn_dict = {}
    for j in range(coreNum):
        message_conn = cd.MySQLCommand()
        message_conn.connectMysql(table="wechat_sender")
        conn_dict["conn_%s" % str(j)] = message_conn

    # 设计一个钩子
    TAG = True
    while TAG:
        # 多线程解析content
        for sender_conn in conn_dict.values():
            message = message_cursor.fetchone()
            # 如果已经遍历结束，直接结束
            if message is not None:
                # message_conn.closeMysql()
                TAG = False
                break
            th = threading.Thread(target=split_content, args=(sender_conn, message, ))
            # print("第", i, "个线程开启")
            th.start()
            th.join()
    # 关闭连接
    for conn_j in conn_dict.values():
        conn_j.closeMysql()


def main():
    startTime = time.time()
    coreNum = input("线程数量：")
    count_sender_by_chatroom(int(coreNum))
    endTime = time.time()
    print('运行时间：%.3f' % (endTime - startTime))


if __name__ == '__main__':
    main()
