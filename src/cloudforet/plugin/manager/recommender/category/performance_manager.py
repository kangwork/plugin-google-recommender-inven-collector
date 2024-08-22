import logging
import requests
import json

from bs4 import BeautifulSoup
from spaceone.inventory.plugin.collector.lib import *
from cloudforet.plugin.config.global_conf import ASSET_URL
from cloudforet.plugin.manager.recommender import RecommenderManager
from cloudforet.plugin.manager.recommender.category import RecommendationManager

_LOGGER = logging.getLogger(__name__)


class PerformanceRecommendationManager(RecommendationManager):
    service = "Performance Recommendation"
    category = "PERFORMANCE"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.cloud_service_type = "Performance"
        self.metadata_path = "plugin/metadata/recommender/recommendation/performance.yaml"

    @classmethod
    def _add_category_specific_data(cls, data: dict, rec: dict):
        return data
