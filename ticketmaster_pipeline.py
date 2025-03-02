from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

from typing import Any, Optional
from datetime import datetime
from dateutil.relativedelta import relativedelta
import urllib
import dlt
from dlt.common.pendulum import pendulum
from dlt.sources.rest_api import (
    RESTAPIConfig,
    check_connection,
    rest_api_resources,
    rest_api_source,
)
@dlt.source(name="ticketmaster")

def ticketmaster(api_key: Optional[str] = dlt.secrets.value) -> Any:
    # Create a REST API configuration for the Ticketmaster API
    # Use RESTAPIConfig to get autocompletion and type checking
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://app.ticketmaster.com/discovery/v2/",       

            # we add an auth config if the auth token is present
            "auth": (
                {   "name":"apikey",
                    "type":"api_key",
                    "api_key": dlt.secrets["api_secret_key"],
                    "location":"query"
                }
            ),
        },
        # The default configuration for all resources and their endpoints
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "merge"
            # "endpoint": {
            #     "params": {
            #         "apikey": "o3bK4TAxbI1ov8yhLCXp72bMkIpgezzQ",
            #     },
            # },
        },
        "resources": [
            # This is a simple resource definition,
            # that uses the endpoint path as a resource name:
            # "pulls",
            # Alternatively, you can define the endpoint as a dictionary
            # {
            #     "name": "pulls", # <- Name of the resource
            #     "endpoint": "pulls",  # <- This is the endpoint path
            # }
            # Or use a more detailed configuration:
            {
                "name": "events",

                "endpoint": {
                    "path": "events.json",
                    "paginator": {
                        "type":"page_number",
                        "maximum_page":50,
                        "total_path":"page.totalPages"
                    },
                
                    
                    
                    # PageNumberPaginator(maximum_page=50, total_path="page.totalPages"),
                    # Query parameters for the endpoint
                    "params": {
                        "size": "20",
                        # "source":"ticketmaster",
                        "includeTest":"no",
                        # "startDateTime":  urllib.parse.unquote((datetime.now() - relativedelta(**{'years': 2})).strftime('%Y-%m-%dT00:00:00Z')),
                        "endDateTime": (datetime.now()).strftime('%Y-%m-%dT00:00:00Z'),
                        "sort":"date,asc"
                        # "direction": "desc",
                        # "state": "open",
                        # Define `since` as a special parameter
                        # to incrementally load data from the API.
                        # This works by getting the updated_at value
                        # from the previous response data and using this value
                        # for the `since` query parameter in the next request.
                        # "since": {
                        #     "type": "incremental",
                        #     "cursor_path": "updated_at",
                        #     "initial_value": pendulum.today().subtract(days=30).to_iso8601_string(),
                        # },
                    },
                },
            },
            # The following is an example of a resource that uses
            # a parent resource (`issues`) to get the `issue_number`
            # and include it in the endpoint path:
            # {
            #     "name": "issue_comments",
            #     "endpoint": {
            #         # The placeholder {issue_number} will be resolved
            #         # from the parent resource
            #         "path": "issues/{issue_number}/comments",
            #         "params": {
            #             # The value of `issue_number` will be taken
            #             # from the `number` field in the `issues` resource
            #             "issue_number": {
            #                 "type": "resolve",
            #                 "resource": "issues",
            #                 "field": "number",
            #             }
            #         },
            #     },
            #     # Include data from `id` field of the parent resource
            #     # in the child data. The field name in the child data
            #     # will be called `_issues_id` (_{resource_name}_{field_name})
            #     "include_from_parent": ["id"],
            # },
        ],
    }

    yield from rest_api_resources(config)


def load_github() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="ticketmaster",
        destination='duckdb',
        dataset_name="events",
    )

    load_info = pipeline.run(ticketmaster())
    print(load_info)  # noqa: T201


# ------------------------------------------
# CHECK CONNECTION ERROR HANDLER
    def check_network_and_authentication() -> None:
        (can_connect, error_msg) = check_connection(
            ticketmaster,
            "events",
        )
        if not can_connect:
            pass  # do something with the error message

    check_network_and_authentication()
# CHECK CONNECTION ERROR HANDLER
# -------------------------------------------



if __name__ == "__main__":
    load_github()
