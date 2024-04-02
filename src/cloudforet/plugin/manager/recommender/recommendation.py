import logging
import requests
import json

from bs4 import BeautifulSoup
from spaceone.inventory.plugin.collector.lib import *

from cloudforet.plugin.config.global_conf import ASSET_URL, RECOMMENDATION_MAP
from cloudforet.plugin.connector.recommender import *
from cloudforet.plugin.manager import ResourceManager

_LOGGER = logging.getLogger(__name__)

_RECOMMENDATION_TYPE_DOCS_URL = "https://cloud.google.com/recommender/docs/recommenders"

_UNAVAILABLE_RECOMMENDER_IDS = [
    "google.cloudbilling.commitment.SpendBasedCommitmentRecommender",
    "google.accounts.security.SecurityKeyRecommender",
    "google.cloudfunctions.PerformanceRecommender",
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
        assets = [asset for asset in cloud_asset_conn.list_assets_in_project()]
        self._create_location_field_to_recommendation_map(assets)

        recommendation_parents = self._create_parents_for_request_params()

        recommendation_conn = RecommendationConnector(
            options=options, secret_data=secret_data, schema=schema
        )

        preprocessed_recommendations = []
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

                if resource := recommendation["content"].get("overview"):
                    display["resource"] = resource

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
                preprocessed_recommendations.append(recommendation)

        recommenders = self._create_recommenders(preprocessed_recommendations)
        collected_recommender_ids = self._list_collected_recommender_ids(recommenders)
        for recommender_id in collected_recommender_ids:
            recommender = self.recommender_map[recommender_id]

            total_cost = 0
            resource_count = 0
            total_priority_level = {
                "Lowest": 0,
                "Second Lowest": 0,
                "Highest": 0,
                "Second Highest": 0,
            }
            for recommendation in recommender["recommendations"]:
                if recommender["category"] == "COST":
                    total_cost += recommendation.get("cost", 0)

                if recommendation.get("affectedResource"):
                    resource_count += 1

                total_priority_level[recommendation.get("priorityLevel")] += 1

            if total_cost:
                recommender["costSavings"] = f"Total ${round(total_cost, 2)}/month"
            if resource_count:
                recommender["resourceCount"] = resource_count

            (
                recommender["state"],
                recommender["primaryPriorityLevel"],
            ) = self._get_state_and_priority(total_priority_level)

            self.set_region_code("global")
            yield make_cloud_service(
                name=recommender["name"],
                cloud_service_type=self.cloud_service_type,
                cloud_service_group=self.cloud_service_group,
                provider=self.provider,
                account=self.project_id,
                data=recommender,
                region_code="global",
                instance_type="",
                instance_size=0,
                reference={
                    "resource_id": recommender["id"],
                    "external_link": f"https://console.cloud.google.com/cloudpubsub/schema/detail/{recommender['id']}?project={self.project_id}",
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

    def _create_parents_for_request_params(self):
        recommendation_parents = []
        for recommender_id, recommender_info in self.recommender_map.items():
            for region_or_zone in recommender_info["locations"]:
                recommendation_parents.append(
                    f"projects/{self.project_id}/locations/{region_or_zone}/recommenders/{recommender_id}"
                )
        return recommendation_parents

    def _create_location_field_to_recommendation_map(self, assets):
        parents_and_locations_map = (
            self._create_parents_and_location_map_by_cloud_asset_api(assets)
        )

        self._add_group_and_service_to_recommender_map()
        self._add_locations_to_recommender_map(parents_and_locations_map)

    @staticmethod
    def _create_parents_and_location_map_by_cloud_asset_api(assets):
        parents_and_locations_map = {}
        for asset in assets:
            asset_type = asset["assetType"]
            locations = asset["resource"].get("location", "global")

            service, cloud_service_type = asset_type.split("/")
            cloud_service_group, postfix = service.split(".", 1)
            cloud_service_type = cloud_service_type.lower()

            if cloud_service_group not in parents_and_locations_map:
                parents_and_locations_map[cloud_service_group] = {}
            else:
                if (
                        cloud_service_type
                        not in parents_and_locations_map[cloud_service_group]
                ):
                    parents_and_locations_map[cloud_service_group][
                        cloud_service_type
                    ] = [locations]
                else:
                    if (
                            locations
                            not in parents_and_locations_map[cloud_service_group][
                        cloud_service_type
                    ]
                    ):
                        parents_and_locations_map[cloud_service_group][
                            cloud_service_type
                        ].append(locations)

        for group, cst_and_locations in parents_and_locations_map.items():
            all_locations = set()
            for cst, locations in cst_and_locations.items():
                for location in locations:
                    all_locations.add(location)
            if all_locations:
                parents_and_locations_map[group]["all_locations"] = list(all_locations)

        return parents_and_locations_map

    @staticmethod
    def _add_group_and_service_to_recommender_map():
        for key, value in RECOMMENDATION_MAP.items():
            prefix, cloud_service_group, cloud_service_type, *others = key.split(".")
            if not (
                    cloud_service_type.endswith("Commitments")
                    or cloud_service_type.endswith("Recommender")
            ):
                if cloud_service_group == "cloudsql":
                    cloud_service_group = "sqladmin"
                RECOMMENDATION_MAP[key]["cloudServiceGroup"] = cloud_service_group
                RECOMMENDATION_MAP[key]["cloudServiceType"] = cloud_service_type.lower()
            else:
                RECOMMENDATION_MAP[key]["cloudServiceGroup"] = cloud_service_group
                RECOMMENDATION_MAP[key]["cloudServiceType"] = None

    def _add_locations_to_recommender_map(self, parents_and_locations_map):
        delete_services = []
        for service, cst in parents_and_locations_map.items():
            if not cst:
                delete_services.append(service)

        for service in delete_services:
            del parents_and_locations_map[service]

        for key, value in RECOMMENDATION_MAP.items():
            cloud_service_group = value["cloudServiceGroup"]
            cloud_service_type = value["cloudServiceType"]

            for service, cst_and_locations in parents_and_locations_map.items():
                if cloud_service_group == service:
                    for service_key, locations in cst_and_locations.items():
                        if cloud_service_type == service_key:
                            RECOMMENDATION_MAP[key]["locations"] = locations

                    if (
                            "locations" not in RECOMMENDATION_MAP[key]
                            and cloud_service_group == "compute"
                    ):
                        RECOMMENDATION_MAP[key]["locations"] = cst_and_locations[
                            "instance"
                        ]

                        if cloud_service_type == "commitment":
                            RECOMMENDATION_MAP[key][
                                "locations"
                            ] = self._change_zone_to_region(
                                cst_and_locations["instance"]
                            )

                    if "locations" not in RECOMMENDATION_MAP[key]:
                        RECOMMENDATION_MAP[key]["locations"] = cst_and_locations[
                            "all_locations"
                        ]

            if "locations" not in RECOMMENDATION_MAP[key]:
                RECOMMENDATION_MAP[key]["locations"] = ["global"]

            if "global" not in RECOMMENDATION_MAP[key]["locations"]:
                RECOMMENDATION_MAP[key]["locations"].append("global")

    @staticmethod
    def _change_zone_to_region(zones):
        regions = []
        for zone in zones:
            region = zone.rsplit("-", 1)[0]
            if region not in regions:
                regions.append(region)
        return regions

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

    @staticmethod
    def _create_recommenders(preprocessed_recommendations):
        recommenders = []
        for pre_recommendation in preprocessed_recommendations:
            redefined_insights = []
            if insights := pre_recommendation["display"]["insights"]:
                for insight in insights:
                    redefined_insights.append(
                        {
                            "description": insight["description"],
                            "severity": insight["severity"],
                            "category": insight["category"],
                        }
                    )

            redefined_recommendations = [
                {
                    "description": pre_recommendation["description"],
                    "state": pre_recommendation["stateInfo"]["state"],
                    "affectedResource": pre_recommendation["display"].get("resource"),
                    "location": pre_recommendation["display"]["location"],
                    "priorityLevel": pre_recommendation["display"]["priorityDisplay"],
                    "operations": pre_recommendation["display"]["operationActions"],
                    "cost": pre_recommendation["display"].get("cost"),
                    "costSavings": pre_recommendation["display"].get("costDescription"),
                    "insights": redefined_insights,
                }
            ]

            recommender = {
                "name": pre_recommendation["display"]["recommenderIdName"],
                "id": pre_recommendation["display"]["recommenderId"],
                "description": pre_recommendation["display"][
                    "recommenderIdDescription"
                ],
                "category": pre_recommendation["primaryImpact"]["category"],
                "recommendations": redefined_recommendations,
            }

            recommenders.append(recommender)
        return recommenders

    def _list_collected_recommender_ids(self, recommenders):
        collected_recommender_ids = []
        for recommender in recommenders:
            recommender_id = recommender["id"]
            if "recommendations" not in self.recommender_map[recommender_id]:
                self.recommender_map[recommender_id].update(recommender)
            else:
                for recommendation in recommender["recommendations"]:
                    self.recommender_map[recommender_id]["recommendations"].append(
                        recommendation
                    )

            if recommender_id not in collected_recommender_ids:
                collected_recommender_ids.append(recommender_id)
        return collected_recommender_ids

    @staticmethod
    def _get_state_and_priority(total_priority_level):
        if total_priority_level["Highest"] > 0:
            return "error", "Highest"

        if total_priority_level["Second Highest"] > 0:
            return "warning", "Second Highest"

        if total_priority_level["Second Lowest"] > 0:
            return "ok", "Second Lowest"
        else:
            return "ok", "Lowest"
