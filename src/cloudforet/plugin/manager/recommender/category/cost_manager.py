import logging
import requests
import json

from bs4 import BeautifulSoup
from spaceone.inventory.plugin.collector.lib import *
from cloudforet.plugin.config.global_conf import ASSET_URL
from cloudforet.plugin.manager.recommender import RecommenderManager
from cloudforet.plugin.manager.recommender.category import RecommendationManager

_LOGGER = logging.getLogger(__name__)


class CostRecommendationManager(RecommendationManager):  # , RecommenderManager):
    service = "Cost Recommendation"
    category = "COST"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.cloud_service_type = "Cost"
        self.metadata_path = "plugin/metadata/recommender/recommendation/cost.yaml"

    @classmethod
    def _add_category_specific_data(cls, data: dict, rec: dict):
        impact = {}
        cost_projection = rec["primaryImpact"].get("costProjection")
        (
            impact["cost"],
            impact["costDescription"],
        ) = cls._change_cost_to_description(cost_projection.get("cost", {}))
        impact["duration"] = cls._convert_to_monthly_str(
            cost_projection.get("duration")
        )
        data["impact"] = impact
        monthly_savings = cls._convert_to_monthly_savings(impact)
        data["monthlySavings"] = monthly_savings
        return data

    @staticmethod
    def _convert_to_monthly_str(time: str) -> str:
        if time == "2592000s":
            return "/month"
        if time.endswith("s"):
            return f"/{int(time[:-1]) / 60 / 24 / 30} months"
        if time.endswith("d"):
            return f"/{int(time[:-1]) / 30} months"
        return f"/{time}"

    @staticmethod
    def _convert_to_monthly_savings(impact: dict) -> float:
        cost = impact.get("cost", 0)
        duration = impact.get("duration", "per month")
        if duration == "/month":
            return cost
        if duration.endswith("months"):
            months = int(duration.split("/")[1].split(" ")[0])
            return cost * months
        return 0

    def _get_overall_impacts(self, recs: list) -> str:
        total_savings = 0
        for rec in recs:
            monthly_savings = rec.get("monthlySavings", 0)
            total_savings += monthly_savings
        overall_impacts = f"Total Monthly Savings: ${total_savings}"
        return overall_impacts
