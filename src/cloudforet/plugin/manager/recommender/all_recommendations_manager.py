import logging
from spaceone.inventory.plugin.collector.lib import *
from cloudforet.plugin.manager.base import ResourceManager
from cloudforet.plugin.config.global_conf import (
    ASSET_URL,
    RECOMMENDATION_TYPE_DOCS_URL,
    UNAVAILABLE_RECOMMENDER_IDS,
)
from abc import abstractmethod
from cloudforet.plugin.connector.recommender.recommendation import (
    RecommendationConnector,
)
from cloudforet.plugin.connector.recommender.cloud_asset import CloudAssetConnector
import requests
from bs4 import BeautifulSoup
from cloudforet.plugin.utils.converter import Converter
_LOGGER = logging.getLogger(__name__)


class AllRecommendationsManager(ResourceManager):
    service = "All Recommendations"
    category = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.recommender_map = {}
        self.project_id = ""
        self.rec_parent_to_recs = {}
        self.all_locations = []
        self.cloud_service_group = "Recommender"
        self.cloud_service_type = "AllRecommendations"
        self.metadata_path = (
            "plugin/metadata/recommender/all_recommendations.yaml"
        )
        self.converter = None

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

    @classmethod
    def _is_category(cls, rec: dict) -> bool:
        return (
            not cls.category  # All Recommendations
            or rec.get("primaryImpact", {}).get("category") == cls.category
        )

    def create_cloud_service(self, options, secret_data, schema):
        cloud_services, error_responses = [], []
        self.project_id = secret_data["project_id"]
        self.converter = Converter()
        self.set_recommendation_id_map_by_crawling()

        cloud_asset_conn = CloudAssetConnector(
            options=options, secret_data=secret_data, schema=schema
        )
        assets = [asset for asset in cloud_asset_conn.list_assets_in_project()]
        self._create_location_field_to_recommendation_map(assets)
        self.all_locations = ["global"]

        recommendation_parents = self._create_parents_for_request_params()

        recommendation_conn = RecommendationConnector(
            options=options, secret_data=secret_data, schema=schema
        )

        for recommendation_parent in recommendation_parents:
            recommendations = recommendation_conn.list_recommendations(
                recommendation_parent
            )
            if recommendations:
                self.rec_parent_to_recs[recommendation_parent] = recommendations

        prd_serv_recs = {}

        for rec_parent in self.rec_parent_to_recs:
            if not self._is_category(self.rec_parent_to_recs[rec_parent][0]):
                continue
            recommender_id = rec_parent.split("/")[-1]
            product, product_service = recommender_id.split(".")[:2]
            product = self.converter.convert_product_or_product_service_name(product)
            product_service = self.converter.convert_product_or_product_service_name(product_service)
            location = rec_parent.split("/locations/")[1].split("/")[0]
            if product not in prd_serv_recs:
                prd_serv_recs[product] = {}
            if product_service not in prd_serv_recs[product]:
                prd_serv_recs[product][product_service] = []

            recommender_info = self.recommender_map.get(recommender_id, {})
            recommender_name = recommender_info.get("name")
            short_description = recommender_info.get("shortDescription")
            for rec in self.rec_parent_to_recs[rec_parent]:
                rec_info = self._parse_recommendation(rec)
                rec_info = self._add_category_specific_data(rec_info, rec)
                rec_info["shortDescription"] = short_description
                rec_info["recommenderName"] = recommender_name
                rec_info["location"] = location
                prd_serv_recs[product][product_service].append(rec_info)

        for product in prd_serv_recs:
            for product_service in prd_serv_recs[product]:
                rec_infos = prd_serv_recs[product][product_service]
                overall_values = self._get_overall_values(rec_infos)
                data = {
                    "recommendations": rec_infos,
                    "overallLocation": overall_values["location"],
                    "overallLastRefreshTime": overall_values["lastRefreshTime"],
                    "overallPriority": overall_values["priority"],
                    "overallStates": overall_values["states"],
                    "overallCategories": overall_values["categories"],
                    "overallImpacts": overall_values["impacts"],
                }
                try:
                    cloud_services.append(
                        make_cloud_service(
                            name=f"{product} > {product_service}",
                            cloud_service_type=self.cloud_service_type,
                            cloud_service_group=self.cloud_service_group,
                            provider=self.provider,
                            account=self.project_id,
                            data=data,
                            region_code=data.get("overallLocation"),
                            instance_type="",
                            instance_size=0,
                            reference={
                                "resource_id": f"{product}.{product_service}",
                                "external_link": f"https://console.cloud.google.com/active-assist/list/cost/\
recommendations?project={self.project_id}",
                            },
                        )
                    )
                except Exception as e:
                    error_responses.append(
                        make_error_response(
                            error=e,
                            provider=self.provider,
                            cloud_service_group=self.cloud_service_group,
                            cloud_service_type=self.cloud_service_type,
                        )
                    )
        return cloud_services, error_responses

    def set_recommendation_id_map_by_crawling(self):
        res = requests.get(RECOMMENDATION_TYPE_DOCS_URL)
        soup = BeautifulSoup(res.content, "html.parser")
        table = soup.find("table")
        rows = table.find_all("tr")

        category, name, recommender_id, short_description, etc = "", "", "", "", ""
        for row in rows:
            cols = row.find_all("td")
            cols = [ele.text.strip() for ele in cols]
            if cols:
                try:
                    category, name, recommender_id, short_description, etc = cols
                except ValueError:
                    try:
                        name, recommender_id, short_description, etc = cols
                    except ValueError:
                        recommender_id, short_description, etc = cols

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
                            if re_id not in UNAVAILABLE_RECOMMENDER_IDS:
                                recommender_ids.append(re_id)
                    else:
                        if recommender_id not in UNAVAILABLE_RECOMMENDER_IDS:
                            recommender_ids = [recommender_id]
                        else:
                            continue

                for recommender_id in recommender_ids:
                    self.recommender_map[recommender_id] = {
                        "category": category,
                        "name": name,
                        "shortDescription": short_description,
                    }

    def _parse_recommendation(self, rec: dict) -> dict:
        data = {
            "name": rec.get("name"),
            "category": rec.get("primaryImpact", {}).get("category"),
            "longDescription": rec.get("description"),
            "subtype": rec.get("recommenderSubtype"),
            "resource": rec.get("content", {})
            .get("operationGroups", [{}])[0]
            .get("operations", [{}])[0]
            .get("resource")
            .split("/")[-1],
            "state": rec.get("stateInfo", {}).get("state"),
            "associatedInsights": [
                insight.get("insight") for insight in rec.get("associatedInsights", [])
            ],
            "priority": rec.get("priority"),
            "lastRefreshTime": rec.get("lastRefreshTime"),
        }

        associated_insights = []
        for insight_dic in rec.get("associatedInsights", []):
            insight_id = insight_dic.get("insight")
            if insight_id:
                associated_insights.append(insight_id)
        data["associatedInsights"] = associated_insights

        impact = rec.get("primaryImpact", {})
        impact_field_names = impact.keys()
        for field_name in impact_field_names:
            if field_name.endswith("Projection"):
                impact = impact.get(field_name)
                break
        data["impact"] = impact
        return data

    @abstractmethod
    def _add_category_specific_data(self, data: dict, rec: dict) -> dict:
        return data

    @staticmethod
    def _extract_location(rec: dict) -> str:
        name = rec.get("name", "")
        return name.split("/locations/")[1].split("/")[0]

    def _get_overall_values(self, recs: list) -> dict:
        locations, last_refresh_times, priorities, states, categories = (
            set(),
            set(),
            {},
            set(),
            set(),
        )
        for rec in recs:
            locations.add(rec.get("location"))
            last_refresh_times.add(rec.get("lastRefreshTime"))
            if not rec.get("lastRefreshTime"):
                return rec
            priority = rec.get("priority")
            if priority in priorities:
                priorities[priority] += 1
            else:
                priorities[priority] = 1
            states.add(rec.get("state"))
            categories.add(rec.get("category"))
        states, categories = list(states), list(categories)
        states.sort()
        categories.sort()
        return {
            "location": locations.pop() if len(locations) == 1 else "global",
            "lastRefreshTime": max(last_refresh_times),
            "priority": self.converter.convert_priority_dict_to_priority_str(priorities),
            "states": states,
            "categories": categories,
            "impacts": self._get_overall_impacts(recs),
        }

    @abstractmethod
    def _get_overall_impacts(self, recs: list) -> str:
        return ""

    def _create_parents_for_request_params(self):
        recommendation_parents = []
        for recommender_id, recommender_info in self.recommender_map.items():
            locations = recommender_info.get("locations", self.all_locations)
            for region_or_zone in locations:
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

    def _add_group_and_service_to_recommender_map(self):
        recommender_map = self.recommender_map
        for key, value in recommender_map.items():
            prefix, cloud_service_group, cloud_service_type, *others = key.split(".")
            if not (
                cloud_service_type.endswith("Commitments")
                or cloud_service_type.endswith("Recommender")
            ):
                if cloud_service_group == "cloudsql":
                    cloud_service_group = "sqladmin"
                recommender_map[key]["cloudServiceGroup"] = cloud_service_group
                recommender_map[key]["cloudServiceType"] = cloud_service_type.lower()
            else:
                recommender_map[key]["cloudServiceGroup"] = cloud_service_group
                recommender_map[key]["cloudServiceType"] = None

    def _add_locations_to_recommender_map(self, parents_and_locations_map):
        recommender_map = self.recommender_map
        delete_services = []
        for service, cst in parents_and_locations_map.items():
            if not cst:
                delete_services.append(service)

        for service in delete_services:
            del parents_and_locations_map[service]

        for key, value in self.recommender_map.items():
            cloud_service_group = value["cloudServiceGroup"]
            cloud_service_type = value["cloudServiceType"]

            for service, cst_and_locations in parents_and_locations_map.items():
                if cloud_service_group == service:
                    for service_key, locations in cst_and_locations.items():
                        if cloud_service_type == service_key:
                            recommender_map[key]["locations"] = locations

                    if (
                        "locations" not in recommender_map[key]
                        and cloud_service_group == "compute"
                    ):
                        recommender_map[key]["locations"] = cst_and_locations[
                            "instance"
                        ]

                        if cloud_service_type == "commitment":
                            recommender_map[key]["locations"] = (
                                self.converter.convert_zone_to_region(
                                    cst_and_locations["instance"]
                                )
                            )

                    if "locations" not in recommender_map[key]:
                        recommender_map[key]["locations"] = cst_and_locations[
                            "all_locations"
                        ]

            if "locations" not in recommender_map[key]:
                recommender_map[key]["locations"] = ["global"]

            if "global" not in recommender_map[key]["locations"]:
                recommender_map[key]["locations"].append("global")
