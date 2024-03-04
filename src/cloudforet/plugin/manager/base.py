import abc
import logging
from spaceone.core.manager import BaseManager
from spaceone.inventory.plugin.collector.lib import *

from cloudforet.plugin.config.global_conf import REGION_INFO

_LOGGER = logging.getLogger(__name__)

__all__ = ["ResourceManager"]


class ResourceManager(BaseManager):
    service = None
    collected_region_codes = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.provider = "google_cloud"
        self.cloud_service_group = ""
        self.cloud_service_type = ""

    @classmethod
    def list_managers(cls):
        return cls.__subclasses__()

    def collect_resources(self, options, secret_data, schema):
        _LOGGER.debug(
            f"[collect_resources] collect Field resources (options: {options})"
        )
        try:
            yield from self.collect_cloud_service_type()
            yield from self.collect_cloud_service(options, secret_data, schema)
            yield from self.collect_region()

        except Exception as e:
            yield make_error_response(
                error=e,
                provider=self.provider,
                cloud_service_group=self.cloud_service_group,
                cloud_service_type=self.cloud_service_type,
            )

    def collect_cloud_service_type(self):
        cloud_service_type = self.create_cloud_service_type()
        yield make_response(
            cloud_service_type=cloud_service_type,
            match_keys=[["name", "group", "provider"]],
            resource_type="inventory.CloudServiceType",
        )

    def collect_cloud_service(self, options, secret_data, schema):
        cloud_services = self.create_cloud_service(options, secret_data, schema)
        for cloud_service in cloud_services:
            yield make_response(
                cloud_service=cloud_service,
                match_keys=[
                    [
                        "reference.resource_id",
                        "provider",
                        "cloud_service_type",
                        "cloud_service_group",
                    ]
                ],
            )

    def collect_region(self):
        for region_code in self.collected_region_codes:
            if region := self.match_region_info(region_code):
                yield make_response(
                    region=region,
                    match_keys=[["region_code", "provider"]],
                    resource_type="inventory.Region",
                )

    def match_region_info(self, region_code):
        match_region_info = REGION_INFO.get(region_code)

        if match_region_info:
            region_info = match_region_info.copy()
            region_info.update(
                {
                    "region_code": region_code,
                    "provider": self.provider,
                }
            )
            return region_info
        return None

    def set_region_code(self, region):
        if region not in REGION_INFO:
            region = "global"

        if region not in self.collected_region_codes:
            self.collected_region_codes.append(region)

    @abc.abstractmethod
    def create_cloud_service_type(self):
        raise NotImplementedError(
            "method `create_cloud_service_type` should be implemented"
        )

    @abc.abstractmethod
    def create_cloud_service(self, options, secret_data, schema):
        raise NotImplementedError("method `create_cloud_service` should be implemented")
