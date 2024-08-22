import logging
import requests
import json

from bs4 import BeautifulSoup
from spaceone.inventory.plugin.collector.lib import *
from cloudforet.plugin.config.global_conf import ASSET_URL
from cloudforet.plugin.manager.recommender import RecommenderManager
from cloudforet.plugin.manager.recommender.category import RecommendationManager

_LOGGER = logging.getLogger(__name__)


class ReliabilityRecommendationManager(RecommendationManager):
    service = "Reliability Recommendation"
    category = "RELIABILITY"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.cloud_service_type = "Reliability"
        self.metadata_path = "plugin/metadata/recommender/recommendation/reliability.yaml"

    @classmethod
    def _add_category_specific_data(cls, data: dict, rec: dict):
        return data

    def _get_impact(self, rec: dict) -> dict:
        return {}

    def _get_overall_impacts(self, recs: list):
        # 얘는 impacts에 쓸 게 없음.
        return ""
