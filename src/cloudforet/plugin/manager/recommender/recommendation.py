import logging
import time
import requests
import json

from bs4 import BeautifulSoup
from spaceone.inventory.plugin.collector.lib import *

from cloudforet.plugin.config.global_conf import (
    ASSET_URL,
    REGION_INFO,
    RECOMMENDATION_MAP,
)
from cloudforet.plugin.connector.recommender import *
from cloudforet.plugin.manager import ResourceManager

_LOGGER = logging.getLogger(__name__)

_RECOMMENDATION_TYPE_DOCS_URL = "https://cloud.google.com/recommender/docs/recommenders"

_UNAVAILABLE_RECOMMENDER_IDS = [
    "google.cloudbilling.commitment.SpendBasedCommitmentRecommender",
    "google.accounts.security.SecurityKeyRecommender",
    "google.cloudfunctions.PerformanceRecommender",
]

_COST_RECOMMENDER_IDS = [
    "google.bigquery.capacityCommitments.Recommender",
    "google.cloudsql.instance.IdleRecommender",
    "google.cloudsql.instance.OverprovisionedRecommender",
    "google.compute.commitment.UsageCommitmentRecommender",
    "google.cloudbilling.commitment.SpendBasedCommitmentRecommender",
    "google.compute.image.IdleResourceRecommender",
    "google.compute.address.IdleResourceRecommender",
    "google.compute.disk.IdleResourceRecommender",
    "google.compute.instance.IdleResourceRecommender",
]


class RecommendationManager(ResourceManager):
    service = "Recommender"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.cloud_service_group = "Recommender"
        self.cloud_service_type = "Recommendation"
        self.metadata_path = "plugin/metadata/recommender/recommendation.yaml"
        self.recommender_map = RECOMMENDATION_MAP
        self.project_id = ""

    def create_cloud_service_type(self):
        return make_cloud_service_type(
            name=self.cloud_service_type,
            group=self.cloud_service_group,
            provider=self.provider,
            metadata_path=self.metadata_path,
            is_primary=True,
            is_major=True,
            service_code="Recommender",
            tags={"spaceone:icon": f"{ASSET_URL}/user_preferences.svg"},
            labels=["Analytics"],
        )

    def create_cloud_service(self, options, secret_data, schema):
        self.project_id = secret_data["project_id"]

        # Needs periodic updating
        # self.recommender_map = self._create_recommendation_id_map_by_crawling()

        cloud_asset_conn = CloudAssetConnector(
            options=options, secret_data=secret_data, schema=schema
        )
        asset_names = [
            asset["name"] for asset in cloud_asset_conn.list_assets_in_project()
        ]

        target_locations = self._create_target_locations(asset_names)
        recommendation_parents = self._create_recommendation_parents(target_locations)

        recommendation_conn = RecommendationConnector(
            options=options, secret_data=secret_data, schema=schema
        )

        for recommendation_parent in recommendation_parents:
            recommendations = recommendation_conn.list_recommendations(
                recommendation_parent
            )
            for recommendation in recommendations:
                recommendation_name = recommendation["name"]
                region, recommender_id = self._get_region_and_recommender_id(
                    recommendation_name
                )

                display = {
                    "recommenderId": recommender_id,
                    "recommenderIdName": self.recommender_map[recommender_id]["name"],
                    "recommenderIdDescription": self.recommender_map[recommender_id][
                        "shortDescription"
                    ],
                    "priorityDisplay": self.convert_readable_priority(
                        recommendation["priority"]
                    ),
                    "overview": json.dumps(recommendation["content"]["overview"]),
                    "operations": json.dumps(
                        recommendation["content"].get("operationGroups", "")
                    ),
                    "operationActions": self._get_actions(recommendation["content"]),
                    "location": self._get_location(recommendation_parent),
                }

                if resource := recommendation["content"]["overview"].get(
                    "resourceName"
                ):
                    display["resource"] = self._change_resource(resource)

                if cost_info := recommendation["primaryImpact"].get("costProjection"):
                    cost = cost_info.get("cost", {})
                    (
                        display["cost"],
                        display["costDescription"],
                    ) = self._change_cost_to_description(cost)

                if insights := recommendation["associatedInsights"]:
                    insight_conn = InsightConnector(
                        options=options, secret_data=secret_data, schema=schema
                    )
                    related_insights = self._list_insights(insights, insight_conn)
                    display["insights"] = self._change_insights(related_insights)

                recommendation.update({"display": display})

                self.set_region_code("global")
                yield make_cloud_service(
                    name=recommendation_name,
                    cloud_service_type=self.cloud_service_type,
                    cloud_service_group=self.cloud_service_group,
                    provider=self.provider,
                    account=self.project_id,
                    data=recommendation,
                    region_code="global",
                    instance_type="",
                    instance_size=0,
                    reference={
                        "resource_id": recommendation_name,
                        "external_link": f"https://console.cloud.google.com/cloudpubsub/schema/detail/{recommendation_name}?project={self.project_id}",
                    },
                )

    @staticmethod
    def _create_recommendation_id_map_by_crawling():
        res = requests.get(_RECOMMENDATION_TYPE_DOCS_URL)
        soup = BeautifulSoup(res.content, "html.parser")
        table = soup.find("table")
        rows = table.find_all("tr")

        recommendation_id_map = {}
        category = ""
        for row in rows:
            cols = row.find_all("td")
            cols = [ele.text.strip() for ele in cols]
            if cols:
                try:
                    category, name, recommender_id, short_description, etc = cols
                except ValueError:
                    name, recommender_id, short_description, etc = cols

                recommender_ids = []
                if "Cloud SQL performance recommender" in name:
                    name = "Cloud SQL performance recommender"
                    short_description = "Improve Cloud SQL instance performance"
                    recommender_ids = [
                        "google.cloudsql.instance.PerformanceRecommender"
                    ]
                else:
                    if recommender_id.count("google.") > 1:
                        re_ids = recommender_id.split("google.")[1:]
                        for re_id in re_ids:
                            re_id = "google." + re_id
                            if re_id not in _UNAVAILABLE_RECOMMENDER_IDS:
                                recommender_ids.append(re_id)
                    else:
                        if recommender_id not in _UNAVAILABLE_RECOMMENDER_IDS:
                            recommender_ids = [recommender_id]
                        else:
                            continue

                for recommender_id in recommender_ids:
                    recommendation_id_map[recommender_id] = {
                        "category": category,
                        "name": name,
                        "shortDescription": short_description,
                    }

        return recommendation_id_map

    @staticmethod
    def _create_target_locations(asset_names):
        locations = []
        for asset_name in asset_names:
            if (
                "locations/" in asset_name
                or "regions/" in asset_name
                and "subnetworks" not in asset_name
            ):
                try:
                    prefix, sub_asset = asset_name.split("locations/")
                    location, _ = sub_asset.split("/", 1)

                    if location not in locations:
                        locations.append(location)

                except ValueError:
                    prefix, sub_asset = asset_name.split("regions/")
                    location, _ = sub_asset.split("/", 1)

                    if location not in locations:
                        locations.append(location)
        return locations

    @staticmethod
    def _select_available_locations(locations):
        available_locations = []
        for location in locations:
            if location[-2:] in ["-a", "-b", "-c"]:
                available_locations.append(location[:-2])

            if location in REGION_INFO:
                available_locations.append(location)

        return available_locations

    def _create_recommendation_parents(self, locations):
        recommendation_parents = []
        locations = self._select_available_locations(locations)
        for location in locations:
            for recommender_id in self.recommender_map.keys():
                if (
                    recommender_id in _COST_RECOMMENDER_IDS
                    and location != "global"
                    and location[-2:] not in ["-a", "-b", "-c"]
                ):
                    regions_and_zones = [
                        location,
                        f"{location}-a",
                        f"{location}-b",
                        f"{location}-c",
                    ]
                    for region_or_zone in regions_and_zones:
                        recommendation_parents.append(
                            f"projects/{self.project_id}/locations/{region_or_zone}/recommenders/{recommender_id}"
                        )
                else:
                    recommendation_parents.append(
                        f"projects/{self.project_id}/locations/{location}/recommenders/{recommender_id}"
                    )

        return recommendation_parents

    @staticmethod
    def _get_region_and_recommender_id(recommendation_name):
        try:
            project_id, resource = recommendation_name.split("locations/")
            region, _, instance_type, _ = resource.split("/", 3)
            return region, instance_type

        except Exception as e:
            _LOGGER.error(
                f"[_get_region] recommendation passing error (data: {recommendation_name}) => {e}",
                exc_info=True,
            )

    @staticmethod
    def convert_readable_priority(priority):
        if priority == "P1":
            return "Highest"
        elif priority == "P2":
            return "Second Highest"
        elif priority == "P3":
            return "Second Lowest"
        elif priority == "P4":
            return "Lowest"
        else:
            return "Unspecified"

    @staticmethod
    def _get_actions(content):
        overview = content.get("overview", {})
        operation_groups = content.get("operationGroups", [])
        actions = ""

        if recommended_action := overview.get("recommendedAction"):
            return recommended_action

        else:
            for operation_group in operation_groups:
                operations = operation_group.get("operations", [])
                for operation in operations:
                    action = operation.get("action", "test")
                    first, others = action[0], action[1:]
                    action = first.upper() + others

                    if action == "Test":
                        continue
                    elif actions:
                        actions += f" and {action}"
                    else:
                        actions += action

            return actions

    @staticmethod
    def _get_location(recommendation_parent):
        try:
            project_id, parent_info = recommendation_parent.split("locations/")
            location, _ = parent_info.split("/", 1)
            return location
        except Exception as e:
            _LOGGER.error(
                f"[get_location] recommendation passing error (data: {recommendation_parent}) => {e}",
                exc_info=True,
            )

    @staticmethod
    def _change_resource(resource):
        try:
            resource_name = resource.split("/")[-1]
            return resource_name
        except ValueError:
            return resource

    @staticmethod
    def _change_cost_to_description(cost):
        currency = cost.get("currencyCode", "USD")
        total_cost = 0

        if nanos := cost.get("nanos", 0):
            if nanos < 0:
                nanos = -nanos / 1000000000
            else:
                nanos = nanos / 1000000000
            total_cost += nanos

        if units := int(cost.get("units", 0)):
            if units < 0:
                units = -units
            total_cost += units

        total_cost = round(total_cost, 2)
        description = f"{total_cost}/month"

        if "USD" in currency:
            currency = "$"
            description = f"{currency}{description}"

        return total_cost, description

    @staticmethod
    def _list_insights(insights, insight_conn):
        related_insights = []
        for insight in insights:
            insight_name = insight["insight"]
            insight = insight_conn.get_insight(insight_name)
            related_insights.append(insight)
        return related_insights

    @staticmethod
    def _change_resource_name(resource):
        try:
            resource_name = resource.split("/")[-1]
            return resource_name
        except ValueError:
            return resource

    def _change_target_resources(self, resources):
        new_target_resources = []
        for resource in resources:
            new_target_resources.append(
                {"name": resource, "displayName": self._change_resource_name(resource)}
            )
        return new_target_resources

    def _change_insights(self, insights):
        changed_insights = []
        for insight in insights:
            changed_insights.append(
                {
                    "name": insight["name"],
                    "description": insight["description"],
                    "lastRefreshTime": insight["lastRefreshTime"],
                    "observationPeriod": insight["observationPeriod"],
                    "state": insight["stateInfo"]["state"],
                    "category": insight["category"],
                    "insightSubtype": insight["insightSubtype"],
                    "severity": insight["severity"],
                    "etag": insight["etag"],
                    "targetResources": self._change_target_resources(
                        insight["targetResources"]
                    ),
                }
            )
        return changed_insights
