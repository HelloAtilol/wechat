# -*- coding: utf-8 -*-

"""
专门处理微信聊天记录的类
createDate : 2019.03.26
updateDate : 2019.03.26
"""

import re


class WechatContent:

    def __init__(self, context):
        # 原始信息
        self.origin_context = context

        # 分解内容用到的正则表达式
        self.re_split = re.compile(r'^(.*)(:\n)(.*)')

        # 分解@用到的正则表达式
        self.re_at = r"@.*?[，\? ]"

    def splitContent(self):
        """
        将内容分解为：id + context
        :return:
        """
        result = {}
        try:
            split_res = self.re_split.match(self.origin_context)
            only_con = self.origin_context.replace(split_res[1] + split_res[2], '')
            result["user_id"] = split_res[1]
        except TypeError:
            result['user_id'] = ''
            only_con = self.origin_context
        result["only_con"] = only_con
        return result

    def splitAt(self):
        """
        解析出来所有的@内容
        :return:
        """
        # 如果包含“<”， 说明存在网页，不需要解析@；
        if "<" in self.origin_context:
            return None
        else:
            split_at = re.findall(self.re_at, self.origin_context, re.M)
            return [sa[1:-1] for sa in split_at]
