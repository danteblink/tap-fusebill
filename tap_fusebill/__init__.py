#!/usr/bin/env python3
import os
import json
import singer
from singer import utils, metadata, Transformer
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema 
import requests 

#Pass in a dictionary to the Headers parameter 
headers = {'Authorization' : 'Basic MDo0REZ0cHBxSDZWanJZc2VCeGZxTDhGc3JpZE96NjVEN3NJRHNPb2dQN1JwUmdqUWFCTnJiaE81OXNPc3FLRklC', 'Content-Type' : 'application/json'} 


REQUIRED_CONFIG_KEYS = ["start_date", "username", "password"]
LOGGER = singer.get_logger()

BOOKMARK_KEYS = {
    'subscriptionSummary': 'createdTimestamp'

}


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def get_discovery_metadata(schema):
    mdata = metadata.new()
    #mdata = metadata.write(mdata, (), 'table-key-properties', stream.key_properties)
    #mdata = metadata.write(mdata, (), 'forced-replication-method', stream.replication_method)

    for field_name in schema['properties'].keys():
        
        mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')

    return metadata.to_list(mdata)

def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        if stream_id == 'subscriptionSummary':
        # TODO: populate any metadata and stream's key properties here..
            stream_metadata = []


            key_properties = ['id']
            streams.append(
                CatalogEntry(
                    tap_stream_id=stream_id,
                    stream=stream_id,
                    schema=schema,
                    key_properties=key_properties,
                    metadata=stream_metadata,
                    replication_key=BOOKMARK_KEYS[stream_id],
                    is_view=None,
                    database=None,
                    table=None,
                    row_count=None,
                    stream_alias=None,
                    replication_method=None
                )
            )
    return Catalog(streams)


def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    
    for stream in catalog.get_selected_streams(state):

        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        bookmark_column = stream.replication_key
        is_sorted = True  # TODO: indicate whether data is sorted ascending on bookmark value

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=stream.key_properties,
        )

        isLastPage = False
        pageNumber = 0

        bookmark = singer.get_bookmark(state, stream.tap_stream_id, bookmark_column)

        bookmark = ''
        
        while not isLastPage:
            
            response =  requests.get('https://secure.fusebill.com/v1/'+ str(stream.tap_stream_id) +'?pageSize=1000&pageNumber='+ str(pageNumber) + '&sortOrder=Ascending&sortExpression=createdTimestamp&query=createdTimestamp:' + bookmark + '|',  headers=headers)

            totalPages = int(response.headers['X-MaxPageIndex'])
    
            tap_data = response.json()

            max_bookmark = None
            
            with Transformer() as transformer:

                for row in tap_data:
                    # TODO: place type conversions or transformations here
                    
                    # write one or more rows to the stream:
                    singer.write_record(stream.tap_stream_id, 
                                        transformer.transform(row,
                                        stream.schema.to_dict(),
                                        metadata.to_map(stream.metadata)))

                    if bookmark_column:
                        if is_sorted:
                            # update bookmark to latest value
                            singer.write_bookmark(state, stream.tap_stream_id, bookmark_column, row[bookmark_column])
                            singer.write_state(state)
                        else:
                            # if data unsorted, save max value until end of writes
                            max_bookmark = max(max_bookmark, row[bookmark_column])

                if bookmark_column and not is_sorted:
                    singer.write_state({stream.tap_stream_id: max_bookmark})


            if pageNumber == totalPages:
                    isLastPage = True
                    
            else:
                pageNumber = pageNumber + 1

    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
       
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
