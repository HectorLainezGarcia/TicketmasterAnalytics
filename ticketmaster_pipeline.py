from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
from dateutil import parser
import pytz
from typing import Any
from datetime import datetime
from dateutil.relativedelta import relativedelta
import dlt
import argparse
from dlt.sources.rest_api import (
    RESTAPIConfig,
    rest_api_resources,
)
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pytz



@dlt.source(name="ticketmaster")

def ticketmaster(start_date=None, end_date=None) -> Any:

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
        },
        "resources": [
            {
                "name": "events",

                "endpoint": {
                    "path": "events.json",
                    "paginator": {
                        "type":"page_number",
                        "maximum_page":50,
                        "total_path":"page.totalPages"
                    },
                    # Query parameters for the endpoint
                    "params": {
                        "size": "20",
                        # "source":"ticketmaster",   #removed to capture all sources
                        "includeTest":"no",
                        "startDateTime":  start_date,  # StartDateTime after this value 
                        "endDateTime": end_date,       # StartDateTime before this value
                        "sort":"date,asc"
                        
                    },
                },
            },
        ],
    }

    yield from rest_api_resources(config)

def get_date_range(start_date=None, end_date=None, period=None, amount=None, timezone='UTC'):
    date_list = []
    tz = pytz.timezone(timezone)
    
    if start_date and end_date:
        # Convert strings to datetime objects if necessary
        if isinstance(start_date, str):
            start_date = datetime.fromisoformat(start_date)
        if isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date)

        current_date = tz.localize(start_date)
        end_date = tz.localize(end_date)

        while current_date <= end_date:
            date_list.append(current_date.strftime('%Y-%m-%dT%H:%M:%SZ'))
            current_date += timedelta(days=1)
    
    elif period and amount:
        end_date = datetime.now(tz)
        start_date = end_date - relativedelta(**{period: amount})

        current_date = start_date
        while current_date <= end_date:
            date_list.append(current_date.strftime('%Y-%m-%dT%H:%M:%SZ'))
            current_date += timedelta(days=1)
    date_list.sort(reverse=True)
    return date_list

def load_incremental_events(start_date=None, end_date=None, period=None, amount=None, timezone='UTC' ) -> None:

    date_range = get_date_range(start_date, end_date, period, amount, timezone='UTC')
    print(date_range)
    pipeline = dlt.pipeline(
        pipeline_name='ticketmaster',
        destination='snowflake',
        dataset_name='landing_incremental'#SCHEMA,
    )

    for date in date_range:
        print(date)
        load_info = pipeline.run(ticketmaster(date, date))
        print(load_info) 



if __name__ == "__main__":


    parser = argparse.ArgumentParser(description='Generate a list of dates in a given range.')
    
    parser.add_argument('--start_date', type=str, help='Start date in ISO format (e.g., 2025-03-01T00:00:00)')
    parser.add_argument('--end_date', type=str, help='End date in ISO format (e.g., 2025-03-05T00:00:00)')
    parser.add_argument('--period', type=str, choices=['days', 'months', 'years'], help='Time period (e.g., days, months, years)')
    parser.add_argument('--amount', type=int, help='Amount of the period to subtract from today')
    parser.add_argument('--timezone', type=str, default='UTC', help='Timezone (default: UTC)')
    
    args = parser.parse_args()
    
    if args.start_date and args.end_date:
        date_range = get_date_range(start_date=args.start_date, end_date=args.end_date, timezone=args.timezone)
        load_incremental_events(start_date=args.start_date, end_date=args.end_date, timezone=args.timezone)
    elif args.period and args.amount:
        date_range = get_date_range(period=args.period, amount=args.amount, timezone=args.timezone)
        load_incremental_events(period=args.period, amount=args.amount, timezone=args.timezone)
    else:
        print("Please provide either start and end dates, or a period and amount.")
        exit(1)
    
    
