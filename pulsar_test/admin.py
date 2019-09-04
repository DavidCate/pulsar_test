"""
Author: xiong

creat at 2019/9/3

"""
import aiohttp
import asyncio

class Admin:
    def __init__(self, ip, port,ispersistent=True, tenant="public", namespace="default"):
        persistent = "persistent"
        if not ispersistent:
            persistent = "non-persistent"
        self.__info = dict()
        self.__info['ip'] = ip
        self.__info['port'] = port
        self.__info['persistent'] = persistent
        self.__info['tenant'] = tenant
        self.__info['namespace'] = namespace
        self.url = "http://{ip}:{port}/admin/v2/{persistent}/{tenant}/{namespace}"\
            .format(**self.__info)
        print(self.url)

    async def getMessageById(self,topic, subscription_name, sequence_id):
        """
        根据消息id获取信息
        :param topic:
        :param subscription_name:
        :param sequence_id:
        :return:
        """
        url = self.url + "/{topic}/subscription/{subName}/position/{messagePosition}"\
            .format(topic=topic,subName=subscription_name, messagePosition=sequence_id)
        print(url)
        async with aiohttp.request('get',url) as res:
            if res.status is 200:
                content = str(await res.content.read(), 'utf-8')
                print('getMessageById', content)
                return content

    async def skip(self, subscription_name, num):
        """
        skip num消息
        :param subscription_name:
        :param num:
        :return:
        """
        url = self.url + "/subscription/{subName}/skip/{numMessages}"\
            .format(subName=subscription_name,numMessages = num)
        print(url)
        async with aiohttp.request('get', url) as res:
            if res.status is 200:
                content = str(await res.content.read(), 'utf-8')
                print('skip', content)
                return content

    async def skip_all(self, subscription_name):
        """
        skip所有消息
        :param subscription_name:
        :return:
        """
        url = self.url + "/subscription/{subName}/skip_all".format(subName = subscription_name)
        print(url)
        async with aiohttp.request('get', url) as res:
            if res.status is 200:
                content = str(await res.content.read(), 'utf-8')
                print('skip_all', content)
                return content

    async def resetCursor(self, subscription_name):
        """
        重置游标
        :param subscription_name:
        :return:
        """
        url = self.url + "/subscription/{subName}/resetcursor".format(subName = subscription_name)
        print(url)
        async with aiohttp.request('get', url) as res:
            if res.status is 200:
                content = str(await res.content.read(), 'utf-8')
                print('resetCursor', content)
                return content

    async def lastMessageId(self,topic):
        """
        获取topic最后一条消息id
        :param topic:
        :return:
        """
        url = self.url + "/{topic}/lastMessageId".format(topic=topic)
        print(url)
        async with aiohttp.request('get', url) as res:
            if res.status is 200:
                content = str(await res.content.read(), 'utf-8')
                print('lastMessageId', content)
                return content

    async def getAllTopic(self):
        """
        获取所有topic
        :return:
        """
        url = self.url
        async with aiohttp.request('get', url) as res:
            if res.status is 200:
                content = str(await res.content.read(), 'utf-8')
                print('getAllTopic', content)
                return content

    async def getBacklog(self,topic):
        """
        获取topic后台日志
        :param topic:
        :return:
        """
        url = self.url + "/{topic}/backlog".format(topic=topic)
        print(url)
        async with aiohttp.request('get', url) as res:
            if res.status is 200:
                content = str(await res.content.readany(), 'utf-8')
                print('getBacklog', content)
                return content

async def main():
    info = {
        'ip':'47.94.142.82',
        'port':16650,
        'ispersistent':True,
    }
    admin = Admin(**info)
    await admin.getMessageById('topic-1','sub-1',1)
    await admin.getAllTopic()
    await admin.getBacklog('topic-1')
    await admin.lastMessageId('topic-1')
    await admin.resetCursor('sub-1')
    await admin.skip('sub-1',2)
    await admin.skip_all('sub-1')

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())