import time
import logging

from spaceone.inventory.plugin.collector.lib.server import CollectorPluginServer

from cloudforet.plugin.manager import ResourceManager

app = CollectorPluginServer()

_LOGGER = logging.getLogger("spaceone")


@app.route("Collector.init")
def collector_init(params: dict) -> dict:
    return _create_init_metadata()


@app.route("Collector.verify")
def collector_verify(params: dict) -> None:
    pass


@app.route("Collector.collect")
def collector_collect(params: dict) -> dict:
    options = params["options"]
    secret_data = params["secret_data"]
    schema = params.get("schema")

    resource_mgrs = ResourceManager.list_managers()
    for manager in resource_mgrs:
        start_time = time.time()
        _LOGGER.debug(f"[START] Collect Resources (Service: {manager.service})")
        results = manager().collect_resources(options, secret_data, schema)

        for result in results:
            yield result

        _LOGGER.debug(
            f"[DONE] service: {manager.service}, manager: {manager} Finished {time.time() - start_time:2f} Seconds"
        )


@app.route("Job.get_tasks")
def job_get_tasks(params: dict) -> dict:
    pass


def _create_init_metadata():
    return {
        "metadata": {
            "supported_resource_type": [
                "inventory.CloudService",
                "inventory.CloudServiceType",
                "inventory.Region",
                "inventory.ErrorResource",
            ],
            "options_schema": {},
        }
    }
