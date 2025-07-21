import logging
import itertools

from google.cloud import resourcemanager_v3
from google.oauth2.service_account import Credentials
from plugin.connector.base_connector import GoogleCloudConnector

__all__ = ["ResourceManagerV3Connector"]

_LOGGER = logging.getLogger(__name__)


class ResourceManagerV3Connector(GoogleCloudConnector):
    def __init__(self, **kwargs):
        self.secret_data = kwargs.get("secret_data", {})
        self.credentials = Credentials.from_service_account_info(self.secret_data)
        self.projects_client = resourcemanager_v3.ProjectsAsyncClient(credentials=self.credentials)
        self.folders_client = resourcemanager_v3.FoldersAsyncClient(credentials=self.credentials)
        self.organizations_client = resourcemanager_v3.OrganizationsAsyncClient(credentials=self.credentials)

    async def list_projects(self, parent: str) -> list:
        request = resourcemanager_v3.ListProjectsRequest(parent=parent)
        pages = await self.projects_client.list_projects(request=request)
        return [project async for project in pages]

    async def get_organization(self, organization_id: str):
        request = resourcemanager_v3.GetOrganizationRequest(name=organization_id)
        return await self.organizations_client.get_organization(request=request)

    async def list_folders(self, parent: str) -> list:
        request = resourcemanager_v3.ListFoldersRequest(parent=parent)
        pages = await self.folders_client.list_folders(request=request)
        return [folder async for folder in pages]

    async def list_role_bindings(self, resource: str) -> list:
        policy = await self.projects_client.get_iam_policy(resource=resource)
        return list(itertools.chain(*[binding.members for binding in policy.bindings]))

    async def search_folders(self) -> list:
        pages = await self.folders_client.search_folders()
        return [folder async for folder in pages]