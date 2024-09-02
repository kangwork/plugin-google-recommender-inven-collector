import logging
from cloudforet.plugin.connector.base import GoogleCloudConnector
from cloudforet.plugin.utils.error_handlers import handle_403_exception

__all__ = ["InsightConnector"]
_LOGGER = logging.getLogger(__name__)


class InsightConnector(GoogleCloudConnector):
    google_client_service = "recommender"
    version = "v1beta1"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @handle_403_exception(default_response={})
    def get_policy_insight(self, insight_id: str, **query):
        insight_parent = f"projects/{self.project_id}/locations/global/insightTypes/google.iam.policy/insights/{insight_id}"
        query.update({"parent": insight_parent})
        request = (
            self.client.projects()
            .locations()
            .insightTypes()
            .insights()
            .get(**query)
        )
        response = request.execute()
        return response

    @handle_403_exception(default_response=[])
    def list_insights(self, insight_parent, **query):
        insights = []
        query.update({"parent": insight_parent})
        request = (
            self.client.projects()
            .locations()
            .insightTypes()
            .insights()
            .list(**query)
        )

        while request is not None:
            response = request.execute()
            insights.extend(
                insight for insight in response.get("insights", [])
            )
            request = (
                self.client.projects()
                .locations()
                .insightTypes()
                .insights()
                .list_next(previous_request=request, previous_response=response)
            )
        return insights
