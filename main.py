import time
from typing import List, Any

import aiohttp
from elasticsearch import AsyncElasticsearch
from snowflake import SnowflakeGenerator

from astrbot.api.event import filter
from astrbot.api.star import Context, Star, register
from astrbot.core import AstrBotConfig

from astrbot.api import logger
from astrbot.core.message.components import Image, At, AtAll, File, Video, Record, \
    BaseMessageComponent, Reply, Plain, Share, Contact, Location, Music, Nodes, Node, Json, Forward
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import AiocqhttpMessageEvent
from astrbot.core.star.filter.event_message_type import EventMessageType
from astrbot.core.star.filter.platform_adapter_type import PlatformAdapterType
from data.plugins.astrbot_plugin_histories_collector.config import HistoriesCollectorConfig
from data.plugins.astrbot_plugin_histories_collector.helper import HistoriesHelper


def _is_type_parseable(component: BaseMessageComponent):
    return isinstance(component, (File, Video, Record, Image, Node, Nodes, At, AtAll, Reply, Plain, Share, Contact, Location, Music, Json, Forward))

def _is_type_downloadable(component: BaseMessageComponent):
    return isinstance(component, (File, Video, Record, Image))

def _get_download_url_by_type(component: BaseMessageComponent) -> str | None:
    if isinstance(component, Video):
        return component.file
    elif isinstance(component, (File, Image)):
        return component.url
    return None

async def _get_http_content_length(url: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=1800) as resp:
            if resp.status != 200:
                raise Exception(f"下载文件失败: {resp.status}")
            return int(resp.headers.get("content-length", 0))

async def _is_regular_file(url: str) -> tuple[bool, str] :
    is_regular = True
    check_result = None
    try:
        total_size = await _get_http_content_length(url)
        if total_size > 50 * 1024 * 1024:
            check_result = "文件大于50MB"
            is_regular = False
    except Exception as e:
        check_result = f"获取文件大小失败: {e}"
        is_regular = False
    return is_regular, check_result

async def _parse_message_chain(chain: List[BaseMessageComponent]) -> list[Any]:
    """解析消息链，提取结构化元素和文本摘要"""
    elements = []
    for comp in chain:
        # 处理不同类型消息组件的内容
        logger.info(f"消息类型: {comp.type}")
        if not _is_type_parseable(comp):
            logger.info("暂不支持处理当前消息类型")
            continue

        if isinstance(comp, Node):
            element = {
                "type": comp.type,
                "data":  {
                    "user_id": comp.uin,
                    "nickname": comp.name,
                    "content": await _parse_message_chain(comp.content),
                }
            }
        elif isinstance(comp, Nodes):
            element = {
                "type": comp.type,
                "messages": [],
            }
            for node in comp.nodes:
                element["messages"].append(await _parse_message_chain(node))
        else:
            warn_message = None
            if _get_download_url_by_type(comp):
                url = _get_download_url_by_type(comp)
                if url and url.startswith("http"):
                    is_regular, warn_message = await _is_regular_file(url)
                    if is_regular:
                        if isinstance(comp, File):
                            await comp.get_file()
                        else:
                            await comp.convert_to_file_path()
            element = await comp.to_dict()
            if warn_message:
                element["warn"] = warn_message
        elements.append(element)
    return elements

@register("astrbot_plugin_histories_collector", "xiaoxue1272", "Astrbot 群消息收集器-QQ(ES版)", "v0.1.0-SNAPSHOT")
class HistoriesCollectorPlugin(Star):

    config: HistoriesCollectorConfig

    helper : HistoriesHelper

    id_generator: SnowflakeGenerator

    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.id_generator = SnowflakeGenerator(instance=0)
        self.config = HistoriesCollectorConfig(config)

    async def initialize(self):
        es_config = self.config.es_config
        logger.info(f"准备连接到elasticsearch, 链接配置信息 :{es_config.__dict__}")
        es = AsyncElasticsearch(
            es_config.hosts,  # 集群节点
            http_compress=True,
            http_auth=(es_config.user, es_config.password),  # 用户名密码
            sniff_on_start=False,  # 启动时发现集群所有节点
            request_timeout=30  # 请求超时时间
        )
        if await es.ping():
            logger.info("es连接成功")
            self.helper = HistoriesHelper(es, self.config)
            await self.helper.initial_required_indices()
        else:
            logger.error("es连接失败")
            await es.close()
            raise Exception("es链接失败")

    @filter.event_message_type(EventMessageType.GROUP_MESSAGE)
    @filter.platform_adapter_type(PlatformAdapterType.AIOCQHTTP)
    async def on_group_message(self, event: AiocqhttpMessageEvent):
        """群消息监听时间"""
        event.plain_result()
        if not self.helper.is_enabled_group(event.get_group_id()):
            return
        raw_message = event.message_obj.raw_message
        group = event.message_obj.group
        if "sender" in raw_message:
            sender = raw_message["sender"]
        else:
            sender = await event.bot.get_group_member_info(group_id=int(group.group_id), user_id=int(event.get_sender_id()))
        es_document = {
            "@timestamp": raw_message["time"],
            "group_id": int(group.group_id),
            "group_name": group.group_name,
            "sender_id": int(sender["user_id"]),
            "sender_name": sender["nickname"],
            "sender_nickname": sender["card"],
            "message": event.get_message_outline(),  # 截取摘要
            "message_extra": await _parse_message_chain(chain=event.get_messages())  # 完整的消息元素列表
        }
        await self.helper.save_message(next(self.id_generator), es_document)

    async def terminate(self):
        """可选择实现异步的插件销毁方法，当插件被卸载/停用时会调用。"""
        if self.helper:
            await self.helper.close()
