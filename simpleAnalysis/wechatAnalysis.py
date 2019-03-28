# -*- coding: utf-8 -*-
"""
1. 将聊天数据按照群号，划分不同的文件 √
2. 统计每个群中发言次数最多的ID,发言长短,平均长度,统计用户发链接或图片的次数(也可以统计发图片链接的次数/总发言数), @次数,被@次数。√
3. 统计每个群的活跃度，和所有群加起来的活跃度，在每天时间上的分布（得出的按照每分钟的效果并不好看，尝试每小时或每半小时！）
4. 统计每个群中，被@的次数
5. 统计四个群中被@的次数，同5
6. 四个群分别提到最多的词(去掉stopwords)
"""
from commonTools import wechatContent, ConnectDatabase as cd
import threading


def split_content(talker_content):

    sender_conn = cd.MySQLCommand()
    sender_conn.connectMysql(table="wechat_sender")

    msgId = talker_content[0]
    message_type = talker_content[1]
    chatroomNum = talker_content[2]
    content = talker_content[3]
    wechat = wechatContent.WechatContent(content)
    result = wechat.splitContent()
    user_id = result["user_id"]

    situation = "WHERE chatroom = '%s' AND username = '%s'" % (chatroomNum, user_id)
    # 信息类型为文字
    if message_type is "1":

        context = result["only_con"]
        at_list = wechat.splitAt()

        # 查找在数据库是否已有记录
        title_list = ["*"]

        sender_cursor = sender_conn.select_order(title_list, situation)
        # res = (57, '5383512475@chatroom', 'wxid_7316d1gk9b1922', 1, 39, None, None, 0, None)
        res = sender_cursor.fetchone()

        if res is None:
            # 没有记录
            new_data = {"chatroom": chatroomNum, "username": user_id, "sendTimes": str(1),
                        "allLength": str(len(context)), "atTimes": str(len(at_list)),
                        "imgTimes": str(0), "urlTimes": str(0)}
            # 新建记录，插入数据
            sender_conn.insertData(new_data, primary_key="")
        else:
            # 已有记录
            update_data = {"sendTimes": str(int(res[3]) + 1), "allLength": str(int(res[4]) + len(context)),
                           "atTimes": str(int(res[7]) + len(at_list))}
            sender_conn.update_database(update_data, situation)

    # 信息类型为图片
    elif message_type is "3":

        title_list = ["sendTimes", "imgTimes"]
        sender_cursor = sender_conn.select_order(title_list, situation)
        res = sender_cursor.fetchone()

        if res is None:
            # 没有记录
            new_data = {"chatroom": chatroomNum, "username": user_id,
                        "allLength": str(0), "atTimes": str(0), "sendTimes": str(1),
                        "imgTimes": str(1), "urlTimes": str(0)}
            # 新建记录，插入数据
            sender_conn.insertData(new_data, primary_key="")
        else:
            # 已有记录
            update_data = {"sendTimes": str(int(res[0]) + 1), "imgTimes": str(int(res[1]) + 1)}
            sender_conn.update_database(update_data, situation)

    # 信息类型为其他类型
    else:
        # 如果解析失败，则为特殊信息，不需要考虑
        if user_id is "":
            return None

        title_list = ["sendTimes", "urlTimes"]
        sender_cursor = sender_conn.select_order(title_list, situation)
        res = sender_cursor.fetchone()

        if res is None:
            # 没有记录
            new_data = {"chatroom": chatroomNum, "username": user_id,
                        "allLength": str(0), "atTimes": str(0), "sendTimes": str(1),
                        "imgTimes": str(0), "urlTimes": str(1)}
            # 新建记录，插入数据
            sender_conn.insertData(new_data, primary_key="")
        else:
            # 已有记录
            update_data = {"sendTimes": str(int(res[0]) + 1), "urlTimes": str(int(res[1]) + 1)}
            sender_conn.update_database(update_data, situation)
    print("***第%s条已处理***" % msgId)
    sender_conn.closeMysql()


def count_sender_by_chatroom(coreNum):
    # 与message表建立数据库连接
    message_conn = cd.MySQLCommand()
    message_conn.connectMysql(table="wechat_message")
    message_cursor = message_conn.select_order(["msgId", "type", "talker", "content"])

    # 设计一个钩子
    TAG = True
    while TAG:

        # 多线程解析content
        for i in range(coreNum):
            message = message_cursor.fetchone()
            # 如果已经遍历结束，直接结束
            if message is None:
                TAG = False
                break
            th = threading.Thread(target=split_content, args=(message, ))
            # print("第", i, "个线程开启")
            th.start()
            th.join()

    # 关闭连接
    message_conn.closeMysql()


def main():
    coreNum = input("线程数量：")
    count_sender_by_chatroom(int(coreNum))


if __name__ == '__main__':
    main()
