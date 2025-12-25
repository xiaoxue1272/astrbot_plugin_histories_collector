from typing import Any


class ESConfig:

    hosts: list[str]
    user: str
    password: str

    def __init__(self, config: dict[str, Any]):
        self.hosts = config['hosts']
        self.user = config['user']
        self.password = config['password']

class HistoriesCollectorConfig:

    es_config: ESConfig
    enable_groups: list[str]
    index_prefix: str

    def __init__(self, config: dict[str, Any] ):
        self.es_config = ESConfig(config.get('es_config'))
        self.index_prefix = config.get('index_prefix')
        self.enable_groups = config.get('enable_groups')

