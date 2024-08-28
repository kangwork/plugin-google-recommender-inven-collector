import logging
from spaceone.core import cache
from cloudforet.plugin.connector.base import GoogleCloudConnector



__all__ = ["IAMConnector"]

_LOGGER = logging.getLogger("spaceone")


class IAMConnector(GoogleCloudConnector):
    google_client_service = "iam"
    version = "v1"

    def list_predefined_roles(self):
        roles = []
        request = self.client.roles().list(pageSize=1000, view='FULL')

        while True:
            response = request.execute()

            roles.extend(response.get("roles", []))

            request = (
                self.client.roles()
                .list_next(previous_request=request, previous_response=response)
            )

            if request is None:
                break

        return roles

    def list_project_roles(self, project_id: str = None):
        parent = f"projects/{project_id}"
        roles = []
        request = self.client.projects().roles().list(parent=parent, pageSize=1000, view='FULL')
        while True:
            response = request.execute()

            roles.extend(response.get("roles", []))

            request = (
                self.client.projects()
                .roles()
                .list_next(previous_request=request, previous_response=response)
            )

            if request is None:
                break

        return roles

    def list_organization_roles(self, resource):
        roles = []
        request = self.client.organizations().roles().list(parent=resource, pageSize=1000, view='FULL')

        while True:
            response = request.execute()
            roles.extend(response.get("roles", []))

            request = (
                self.client.organizations()
                .roles()
                .list_next(previous_request=request, previous_response=response)
            )

            if request is None:
                break

        return roles

    def get_all_roles_to_permissions_dict(self, project_id: str, organization_id: str):
        roles_to_permissions = {}
        roles = self.list_predefined_roles()
        roles.extend(self.list_project_roles(project_id))
        if organization_id:
            roles.extend(self.list_organization_roles(organization_id))
        for role in roles:
            roles_to_permissions[role.get("name")] = role.get("includedPermissions", [])
        return roles_to_permissions