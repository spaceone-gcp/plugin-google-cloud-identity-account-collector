import asyncio
import fnmatch
import logging

from google.cloud import resourcemanager_v3
from spaceone.core.manager import BaseManager
from plugin.connector.resource_manager_v1_connector import ResourceManagerV1Connector
from plugin.connector.resource_manager_v3_connector import ResourceManagerV3Connector

_LOGGER = logging.getLogger("spaceone")


class AccountCollectorManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.options = kwargs["options"]
        self.trusting_organization = self.options.get("trusting_organization", True)
        self.exclude_projects = self.options.get("exclude_projects", [])
        self.exclude_folders = self.options.get("exclude_folders", [])
        self.exclude_folders = [
            str(int(folder_id)) for folder_id in self.exclude_folders
        ]

        self.secret_data = kwargs["secret_data"]
        self.trusted_service_account = self.secret_data["client_email"]

        self.resource_manager_v1_connector = ResourceManagerV1Connector(
            secret_data=self.secret_data
        )
        self.resource_manager_v3_connector = ResourceManagerV3Connector(
            secret_data=self.secret_data
        )
        self.final_results = []

    def sync(self) -> dict:
        initial_projects = self.resource_manager_v1_connector.list_projects()
        return asyncio.run(self._sync_internal(initial_projects))

    async def _sync_internal(self, initial_projects: list) -> dict:
        organization_info = await self._get_organization_info(initial_projects)

        if not organization_info:
            raise Exception("[sync] The Organization belonging to this ServiceAccount cannot be found.")
        parents_to_process = [(organization_info.name, [])]

        while parents_to_process:
            tasks = [
                self._process_one_parent(parent, locations)
                for parent, locations in parents_to_process
            ]
            level_results = await asyncio.gather(*tasks, return_exceptions=True)

            parents_to_process = []
            for result in level_results:
                if isinstance(result, Exception):
                    _LOGGER.error(f"[sync] Error processing a level: {result}")
                    continue

                projects, sub_folders = result
                self.final_results.extend(projects)
                parents_to_process.extend(sub_folders)

        return {'results': self.final_results}

    async def _get_organization_info(self, projects_info):
        organization_info = None
        organization_parent = None
        for project_info in projects_info:
            if organization_info:
                break
            parent = project_info.get("parent")
            if parent and parent.get("type") == "organization" and not organization_parent:
                organization_parent = f"organizations/{parent['id']}"
                organization_info = await self.resource_manager_v3_connector.get_organization(organization_parent)
        if not organization_info:
            all_folders = await self.resource_manager_v3_connector.search_folders()
            for folder_info in all_folders:
                parent = folder_info.parent
                if parent.startswith("organizations"):
                    organization_parent = parent
                    organization_info = await self.resource_manager_v3_connector.get_organization(organization_parent)
                    break

        if organization_info:
            _LOGGER.debug(f"[sync] Organization information to sync: {organization_info}")

        return organization_info

    async def _process_one_parent(self, parent: str, locations: list) -> tuple:
        project_task = asyncio.create_task(self.resource_manager_v3_connector.list_projects(parent))
        folder_task = asyncio.create_task(self.resource_manager_v3_connector.list_folders(parent))

        projects_info = await project_task
        folders_info = await folder_task

        processed_projects = await self._process_project_list(projects_info, locations)

        sub_parents_to_process = []
        for folder_info in folders_info:
            folder_parent = folder_info.name
            _, folder_id = folder_parent.split("/")
            if folder_id not in self.exclude_folders:
                next_locations = locations + [{"name": folder_info.display_name, "resource_id": folder_parent}]
                sub_parents_to_process.append((folder_parent, next_locations))

        return processed_projects, sub_parents_to_process

    async def _process_project_list(self, projects_info: list, locations: list) -> list:
        results = []
        trust_check_tasks = {}

        for project_info in projects_info:
            project_id = project_info.project_id
            if project_info.state == resourcemanager_v3.Project.State.ACTIVE and self._check_exclude_project(project_id):
                if self.trusting_organization:
                    results.append(self._make_result(project_info, locations))

                    _LOGGER.debug(
                        f"[sync] ServiceAccount is Trusted with Organization (ServiceAccount: {self.trusted_service_account}, Project ID: {project_id})"
                    )
                else:
                    task = asyncio.create_task(self._is_trusting_project(project_id))
                    trust_check_tasks[project_id] = (task, project_info)

        if trust_check_tasks:
            await asyncio.gather(*[task for task, _ in trust_check_tasks.values()])
            for project_id, (task, project_info) in trust_check_tasks.items():
                is_trusted = task.result()
                results.append(self._make_result(project_info, locations, is_secret_data=is_trusted))

        return results

    async def _is_trusting_project(self, project_id: str) -> bool:
        try:
            role_bindings = await self.resource_manager_v3_connector.list_role_bindings(
                resource=f"projects/{project_id}")
            return f"serviceAccount:{self.trusted_service_account}" in role_bindings
        except Exception as e:
            _LOGGER.error(f"[sync] failed to get role_bindings for {project_id} => {e}")
            return False

    @staticmethod
    def _make_result(project_info, locations, is_secret_data=True):
        project_id = project_info.project_id
        project_name = project_info.display_name
        project_tags = dict(project_info.labels) # MapField를 dict로 변환
        result = {
            "name": project_name,
            "data": {"project_id": project_id},
            "secret_schema_id": "google-secret-project-id",
            "resource_id": project_id,
            "tags": project_tags,
            "location": locations,
        }
        if is_secret_data:
            result["secret_data"] = {"project_id": project_id}
        return result

    def _check_exclude_project(self, project_id):
        for exclude_project_id in self.exclude_projects:
            if fnmatch.fnmatch(project_id, exclude_project_id):
                return False
        return True