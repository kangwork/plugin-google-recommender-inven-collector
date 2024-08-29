import logging
from spaceone.inventory.plugin.collector.lib import *
from cloudforet.plugin.config.global_conf import ASSET_URL
from cloudforet.plugin.connector.recommender.insight import InsightConnector
from cloudforet.plugin.connector.iam import IAMConnector
from cloudforet.plugin.connector.recommender.recommendation import (
    RecommendationConnector,
)
from cloudforet.plugin.manager import ResourceManager
from cloudforet.plugin.utils.converter import Converter

_LOGGER = logging.getLogger(__name__)


class SecurityIAMRecommendationManager(ResourceManager):
    service = "Security Recommendation - IAM Management"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cloud_service_group = "Recommender"
        self.cloud_service_type = "SecurityIAMManagement"
        self.metadata_path = (
            "plugin/metadata/recommender/recommendation/security_iam_management.yaml"
        )
        self.project_id = None
        self.organization_id = None
        self.all_roles_to_permissions = {}
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

    def create_cloud_service(self, options, secret_data, schema):
        cloud_services = []
        error_responses = []
        self.project_id = secret_data.get("project_id")
        self.organization_id = secret_data.get("organization_id")
        member_to_role_to_data = {}
        member_to_overall_values = {}
        iam_connector = IAMConnector(
            options=options, secret_data=secret_data, schema=schema
        )
        self.all_roles_to_permissions = iam_connector.get_all_roles_to_permissions_dict(
            project_id=self.project_id, organization_id=self.organization_id
        )
        revoked_policy_insights, revoked_service_account_insights = self._list_insights(
            options, secret_data, schema
        )
        recs = self.list_recommendations(options, secret_data, schema)
        self.converter = Converter()

        for rec in recs:
            data = self._parse_recommendation(rec)
            member_id = data.pop("memberId")
            member_type = data.pop("memberType")
            role_name = data.pop("roleName")
            if member_id not in member_to_role_to_data:
                member_to_role_to_data[member_id] = {}
                member_to_overall_values[member_id] = {
                    "totalUnusedPermissionsCount": 0,
                    "rolesCount": 0,
                    "_priority_count": 0,
                    "_priority_sum": 0,
                    "insightSubtypes": ["PERMISSIONS USAGE"],
                    "memberType": member_type,
                }

            if role_name in member_to_role_to_data[member_id]:
                for rest_data in member_to_role_to_data[member_id][role_name]:
                    if rest_data["lastRefreshTime"] < data["lastRefreshTime"]:
                        member_to_overall_values[member_id][
                            "totalUnusedPermissionsCount"
                        ] -= rest_data["unusedPermissionsCount"]
                        member_to_overall_values[member_id]["rolesCount"] -= 1
                        member_to_overall_values[member_id]["_priority_count"] -= 1
                        member_to_overall_values[member_id]["_priority_sum"] -= int(
                            rest_data["priority"][1]
                        )
                        break

            member_to_role_to_data[member_id][role_name] = data
            member_to_overall_values[member_id]["totalUnusedPermissionsCount"] += data[
                "unusedPermissionsCount"
            ]
            member_to_overall_values[member_id]["rolesCount"] += 1
            member_to_overall_values[member_id]["_priority_count"] += 1
            member_to_overall_values[member_id]["_priority_sum"] += int(
                data["priority"][1]
            )

        for insight in revoked_policy_insights:
            perm_data = self._parse_permission_usage_insights(insight)
            member = perm_data.get("memberId")
            if member in member_to_role_to_data:
                role = perm_data.get("roleName")
                if role not in member_to_role_to_data[member]:
                    member_to_role_to_data[member][role] = {}
                member_to_role_to_data[member][role].update(
                    perm_data["insightSpecificData"]
                )

        for insight in revoked_service_account_insights:
            service_account_data = self._parse_service_account_insights(insight)
            member = service_account_data.get("memberId")
            member_type = service_account_data.get("memberType")
            insight_data = service_account_data.get("insightSpecificData")
            if member in member_to_role_to_data:
                member_to_overall_values[member]["insightSubtypes"].append(
                    "SERVICE ACCOUNT USAGE"
                )
                member_to_overall_values[member]["lastRefreshTime"] = insight_data[
                    "lastRefreshTime"
                ]
            else:
                member_to_role_to_data[member] = {}
                service_account_data["priority"] = "P4"
                member_to_overall_values[member] = {
                    "_priority_count": 1,
                    "_priority_sum": 4,
                    "insightSubtypes": ["SERVICE ACCOUNT USAGE"],
                    "memberType": member_type,
                }
            member_to_role_to_data[member]["serviceAccount"] = insight_data

        for member in member_to_role_to_data:
            avg_priority = member_to_overall_values[member].pop(
                "_priority_sum"
            ) / member_to_overall_values[member].pop("_priority_count")
            member_to_overall_values[member]["priority"] = (
                self.converter._convert_avg_priority_to_priority(avg_priority)
            )
            data = {
                "serviceAccountRecommendation": member_to_role_to_data[member].pop(
                    "serviceAccount", {}
                ),
                "roleRecommendations": [
                    member_to_role_to_data[member][role]
                    for role in member_to_role_to_data[member]
                ],
                "memberType": member_to_overall_values[member].pop("memberType"),
                "category": "SECURITY",
                "product": "IAM",
                "productCategory": "Access Management",
                "insightSubtypes": member_to_overall_values[member].pop(
                    "insightSubtypes"
                ),
                "overallValues": member_to_overall_values[member],
            }
            try:
                cloud_services.append(
                    make_cloud_service(
                        name=member,
                        cloud_service_type=self.cloud_service_type,
                        cloud_service_group=self.cloud_service_group,
                        provider=self.provider,
                        account=self.project_id,
                        data=data,
                        region_code="global",
                        instance_type="",
                        instance_size=0,
                        reference={
                            "resource_id": member,
                            "external_link": f"https://console.cloud.google.com/active-assist/list/security/recommendations?project={self.project_id}",
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

    def list_recommendations(self, options, secret_data, schema) -> list:
        rec_parents = self._list_recommendation_parents()
        recommendation_conn = RecommendationConnector(
            options=options, secret_data=secret_data, schema=schema
        )
        recs = []
        for parent in rec_parents:
            recs.extend(recommendation_conn.list_recommendations(parent))
        return recs

    def _list_recommendation_parents(self) -> list:
        rec_parents = []
        rec_id = "google.iam.policy.Recommender"
        if self.organization_id:
            rec_parents.append(
                f"organizations/{self.organization_id}/locations/global/recommenders/{rec_id}"
            )
        if self.project_id:
            rec_parents.append(
                f"projects/{self.project_id}/locations/global/recommenders/{rec_id}"
            )
        return rec_parents

    def _parse_recommendation(self, rec: dict) -> dict:
        overview = rec.get("content", {}).get("overview", {})
        member = overview.get("member", ":")
        member_id = member.split(":")[1]
        member_type = member.split(":")[0].upper()
        if member_type == "SERVICEACCOUNT":
            member_type = "SERVICE ACCOUNT"
        data = {
            "memberType": member_type,
            "memberId": member_id,
            "roleName": overview.get("removedRole"),
            "unusedPermissionsCount": rec.get("primaryImpact", {})
            .get("securityProjection", {})
            .get("details", {})
            .get("revokedIamPermissionsCount", 0),
            "lastRefreshTime": rec.get("lastRefreshTime"),
            "priority": rec.get("priority"),
            "insightId": rec.get("associatedInsights", [{}])[0].get("insight"),
        }
        return data

    def _parse_permission_usage_insights(self, insight: dict) -> dict:
        content = insight.get("content", {})
        member = content.get("member", ":")
        member_type, member_id = member.split(":")
        member_type = " ".join([x for x in member_id if x.isupper()])
        member_type = member_type.upper()
        observation_period_in_sec = insight.get("observationPeriod")
        observation_period_in_days = round(int(observation_period_in_sec[:-1]) / 86400)
        role_name = content.get("role")

        all_perms = set(self.all_roles_to_permissions.get(role_name, []))
        _exercised_perms_dict = content.get("exercisedPermissions", [])

        exercised_perms = [perm.get("permission") for perm in _exercised_perms_dict]
        unused_permissions = all_perms.difference(set(exercised_perms))

        inferred_perms = [
            perm.get("permission") for perm in content.get("inferredPermissions", [])
        ]
        data = {
            "memberType": member_type,
            "memberId": member_id,
            "roleName": role_name,
            "insightSpecificData": {
                "roleName": role_name,
                "insightId": insight.get("name"),
                "unusedPermissions": list(unused_permissions),
                "exercisedPermissions": exercised_perms,
                "exercisedPermissionsCount": len(
                    content.get("exercisedPermissions", [])
                ),
                "inferredPermissions": inferred_perms,
                "inferredPermissionsCount": len(inferred_perms),
                "currentTotalPermissionsCount": content.get(
                    "currentTotalPermissionsCount", 0
                ),
                "observationPeriod": observation_period_in_days,
            },
        }
        return data

    def _parse_service_account_insights(self, insight: dict) -> dict:
        content = insight.get("content", {})
        observation_period_in_sec = insight.get("observationPeriod")
        observation_period_in_days = round(int(observation_period_in_sec[:-1]) / 86400)
        data = {
            "memberType": "SERVICE ACCOUNT",
            "memberId": content.get("email"),
            "insightSpecificData": {
                "insightId": insight.get("name"),
                "lastAuthenticatedTime": content.get("lastAuthenticatedTime"),
                "lastRefreshTime": insight.get("lastRefreshTime"),
                "observationPeriod": observation_period_in_days,
            },
        }
        return data

    def _list_insights(self, options, secret_data, schema) -> (list, list):
        insight_connector = InsightConnector(
            options=options, secret_data=secret_data, schema=schema
        )
        insight_parent = f"projects/{self.project_id}/locations/global/insightTypes/"
        revoked_policy_insights = insight_connector.list_insights(
            insight_parent + "google.iam.policy.Insight"
        )
        revoked_service_account_insights = insight_connector.list_insights(
            insight_parent + "google.iam.serviceAccount.Insight"
        )
        return revoked_policy_insights, revoked_service_account_insights
