# -*- coding: utf-8 -*-

"""
读取数据库，按照群划分不同的txt。
"""
import ConnectDatabase as cd
import re
import time
import threading


def re_context(context):
    """
    使用正则表达式分解信息
    :param context:
    :return:分解后的结果，为一个字典，dict{"user_id":"用户ID"(分解失败时为""), "content":"内容", "at":(at用户组成的元组)}
    """
    # 将内容分解为ID与内容的正则表达式
    re_con = re.compile(r'^(.*)(:\n)(.*)')
    result = {}
    try:
        split_res = re_con.match(context)
        only_con = context.replace(split_res[1] + split_res[2], '')
        result["user_id"] = split_res[1]
    except TypeError:
        result['user_id'] = ''
        only_con = context

    result["content"] = only_con
    if "<" in only_con:
        return result
    if "@" in only_con:
        split_at = re.findall(r'@.*?[，\? ]', only_con, re.M)
        result["at"] = split_at
    return result


def getChatroomContent(message_conn, contact_conn, chatroomNum):
    """
    根据聊天群编号将聊天记录转化为txt文件
    :param contact_conn: contact表数据库连接
    :param message_conn: message表数据库连接
    :param chatroomNum: 群号
    :return:
    """

    # 获取聊天群名称
    chatroomName = contact_conn.select_order(["nickname"], "WHERE username = '%s'" % chatroomNum).fetchone()[0]
    # 聊天记录编号
    i = 1
    title_list = ["content", "createTime"]
    situation = "WHERE talker = '%s'" % chatroomNum
    cursor = message_conn.select_order(title_list, situation, order_title="createTime")
    result_file = open("result/%s.txt" % chatroomName, 'a', encoding='utf-8')
    result = cursor.fetchone()
    while result is not None:
        context, createTime = result
        # 将信息时间格式化

        information_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(createTime) / 1000))
        # print("发送信息的时间：", information_time)
        split_result = re_context(context)
        user_id = split_result['user_id']
        try:

            user_name = contact_conn.select_order(["nickname"], "WHERE username = '%s'" % user_id).fetchone()[0]
        except TypeError:
            user_name = user_id
        content = split_result['content']
        line = information_time + "\t" + user_name + "\n" + content + "\n"
        result_file.write(line)
        # 读取下一行
        result = cursor.fetchone()
        print(chatroomName, "已导入", i, "条数据！")
        i += 1
        # break
    result_file.close()


def getFile(chatroomNum):
    """
    通过聊天群编号获得聊天记录
    :param chatroomNum:
    :return:None
    """
    # 建立message表数据库连接
    message_conn = cd.MySQLCommand()
    message_conn.connectMysql(table="wechat_message")
    # 建立contact表数据库连接
    contact_conn = cd.MySQLCommand()
    contact_conn.connectMysql(table="wechat_contact")
    getChatroomContent(message_conn, contact_conn, chatroomNum)
    message_conn.closeMysql()
    contact_conn.closeMysql()


def multiThread():
    """
    多线程启动，降低IO延时造成的问题；
    :return:None
    """
    # 建立message表数据库连接
    message_conn = cd.MySQLCommand()
    message_conn.connectMysql(table="wechat_message")
    chatroomNums = message_conn.select_distinct()
    for chatroomNum in chatroomNums:
        chatroomNum = chatroomNum[0]
        # 判断talker是否为群编号，例如“weixin”这样的私人信息要排除
        if "chatroom" not in chatroomNum:
            continue
        th = threading.Thread(target=getFile, args=(chatroomNum,))
        th.start()
    message_conn.closeMysql()


if __name__ == '__main__':
    multiThread()
