from elasticsearch import AsyncElasticsearch

from astrbot.api import logger
from data.plugins.astrbot_plugin_histories_collector.config import HistoriesCollectorConfig


class HistoriesHelper:

    __ilm_policy_name: str

    __ilm_policy_body = {
        "phases": {
            "hot": {
                "actions": {
                    "rollover": {
                        "max_docs": 2000000,
                        "max_size": "50gb"
                    }
                }
            }
        }
    }

    __index_alias = "qq_messages"

    __index_mappings = {
        "properties": {
            "@timestamp": {"type": "date"},
            "group_id": {"type": "keyword"},
            "group_name": {"type": "text"},
            "sender_id": {"type": "keyword"},
            "sender_name": {"type": "text"},
            "sender_nickname": {"type": "text"},
            "message": {"type": "text"},
            "message_extra": {"type": "nested"}
        }
    }


    def __init__(self, es_client: AsyncElasticsearch, config: HistoriesCollectorConfig):
        self.__es_client = es_client
        self.__config = config
        self.__ilm_policy_name = f"{self.__config.index_prefix}-policy"
        self.index_settings = {
            "number_of_shards": 1,
            "number_of_replicas": 1,
            "index.lifecycle.name": self.__ilm_policy_name,
            "index.lifecycle.rollover_alias": self.__index_alias,
            "analysis": {
                "analyzer": {
                    "default": {
                        "type": "custom",
                        "char_filter": ["html_strip"],
                        "tokenizer": "ik_max_word",
                        "filter": ["lowercase", "trim"]
                    },
                    "default_search": {
                        "type": "custom",
                        "char_filter": ["html_strip"],
                        "tokenizer": "ik_smart",
                        "filter": ["lowercase", "trim"]
                    }
                }
            }
        }

    def is_enabled_group(self, group_id: str) -> bool:
        return group_id in self.__config.enable_groups

    async def initial_required_indices(self):
        """创建ILM策略和索引模板（如果不存在）"""
        try:
            await self.__es_client.ilm.put_lifecycle(name=self.__ilm_policy_name, policy=self.__ilm_policy_body)
            logger.info(f"ILM策略 '{self.__ilm_policy_name}' 已创建/更新")
        except Exception as e:
            logger.warning(f"创建ILM策略失败（可能是版本不支持或已存在）: {e}")
            raise e

        # 2. 创建索引模板：匹配 qq_messages-* 的索引自动应用设置
        index_template_name = f"{self.__config.index_prefix}-template"
        index_template_body = {
            "index_patterns": f"{self.__config.index_prefix}-*",
            "template": {
                "settings": self.index_settings,
                "mappings": self.__index_mappings,
                "aliases": {
                    self.__index_alias: {}
                }
            }
        }
        try:
            await self.__es_client.indices.put_index_template(name=index_template_name, body=index_template_body)
            logger.info(f"索引模板 '{index_template_name}' 已创建/更新")
        except Exception as e:
            logger.warning(f"创建索引模板失败: {e}")
            raise e

        await self._ensure_write_index_exists()

    async def _ensure_write_index_exists(self):
        """
        确保当前有一个初始的、可写的索引。
        如果系统是全新的，需要手动创建第一个索引并初始化别名。
        """

        # 检查写入别名是否已经指向某个存在的索引
        try:
            alias_info = await self.__es_client.indices.get_alias(name=self.__index_alias)
            if alias_info:
                logger.info(f"写入别名 '{self.__index_alias}' 已指向索引: {list(alias_info.keys())}")
                return  # 别名已存在，无需创建
        except Exception:
            # 别名不存在，是全新部署
            pass

        initial_index_name = f"{self.__config.index_prefix}-000001"
        # 别名不存在，需要创建第一个索引并绑定别名
        logger.info(f"初始化第一个索引: {initial_index_name}")
        initial_index_body = {
            "settings": self.index_settings,
            "mappings": self.__index_mappings,
            "aliases": {
                self.__index_alias: {
                    "is_write_index": True  # 明确指定这个索引是别名的当前可写索引
                }
            }
        }
        try:
            await self.__es_client.indices.create(index=initial_index_name, body=initial_index_body)
            logger.info(f"已创建初始索引 '{initial_index_name}' 并绑定写入别名 '{self.__index_alias}'")
        except Exception as e:
            # 索引可能已以其他方式存在，尝试直接绑定别名
            logger.warning(f"创建索引失败: {e}")
            raise e

    async def save_message(self, doc_id, doc_body):
        response = await self.__es_client.create(index=self.__index_alias, id=doc_id, document=doc_body, require_alias=True, human=True)
        if not response.get("result") in ["created", "updated"]:
            raise Exception(f"保存消息到ES失败: \n response: {response} \n document: {doc_body}")

    async def close(self):
        await self.__es_client.close()
