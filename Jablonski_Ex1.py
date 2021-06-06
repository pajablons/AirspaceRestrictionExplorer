import requests
import datetime
import sys
import os
import geopandas
import pandas as pd
import Jablonski_CLI_Arg_Handler as arg_handler


# Primary function for accessing the relevant flight restriction file for a given date, returning a geodataframe
# representation. If the relevant file has not already been downloaded, signals for the file to be retrieved from
# the FAA website.  Defaults to searching in ./nasr/, though this is command argument configurable.
# If a specific query file is provided, that overrides all date-based file selection and that file is immediately
# retrieved.  This query file must still be in the same format as the downloaded files.
def load_nasr_shapefile(date, nasr_path="./nasr/", force_file=None):
    filepath = None
    # First check for and handle if we have a force file
    if force_file:
        filepath = force_file
    else:
        # Craft a system path of the form nasr_path/MODIFIED_DATE.zip
        filepath = os.path.join(nasr_path, "{}.zip".format(date))
        # If the file doesn't already exist, initiate downloading from the remote site
        if not os.path.exists(filepath):
            retrieve_nasr_shapefile(date, nasr_path)
    # The zip archive contains a single folder with a single shapefile in it.  We can extract this directly into
    # a geodataframe, though we do need to do some minor cleanup to fix typing for later use and analysis.
    gdf = geopandas.read_file("zip://{}!Shape_Files".format(filepath))
    gdf['LOWER_VAL'] = gdf['LOWER_VAL'].astype(int)
    return gdf


# Retrieves the flight restriction archive from the FAA website
def retrieve_nasr_shapefile(date, dl_path="./nasr/"):
    # Remote url is generally static, with only the date changing between subscription archives.
    remote = "https://nfdc.faa.gov/webContent/28DaySub/{}/class_airspace_shape_files.zip".format(str(date))

    # Status check to confirm a successful download
    result = requests.get(remote)
    if not result.status_code == 200:
        print("Error in file download.\nUrl used: {}\nRemote returned code: {}".format(remote, result.status_code))
        sys.exit()

    # Create the directory tree for the archive path, if it does not already exist
    if not os.path.exists(dl_path):
        os.makedirs(dl_path)

    # Write the downloaded binary data to a local zip file.
    # Zip file is in format: DL_PATH/DATE.zip
    with open("{}{}.zip".format(dl_path, date), 'wb') as outFile:
        outFile.write(result.content)


# Establishes initial values for configurable parameters, and parses user provided arguments into appropriate formats
def get_arg_dict():
    default_args = {
        'date': datetime.date.today(),  # default date is the current date
        'queryFile': None,              # force a specific restriction file (must be same format as downloaded zips)
        'locations': None,              # locations to be tested (point feature class)
        'outFile': 'results.csv'        # file for legality results to be written to (csv)
    }

    required_args = ['locations']       # The only required argument is our input locations

    arg_dict, rcvd = arg_handler.retrieve_args(defaults=default_args, required=required_args)

    # If a date was provided (must be in YYYY-MM-DD format), parse the string to a date object
    if 'date' in rcvd:
        try:
            arg_dict['date'] = datetime.datetime.strptime(arg_dict['date'], "%Y-%m-%d").date()
        except ValueError:
            print("Illegal date format.  Must be in format YYYY-MM-DD")
            sys.exit()
    return arg_dict


# Determines which of the many restricted flight zones will be relevant for our analysis.
# Easily extensible function for future changes
# Params:
#   base: Initial unfiltered flight restriction geodataframe
# Returns: Filtered geodataframe
def apply_relevancy_filters(base):
    # Drone flight can be naively assumed to be up to 400' above ground level.  This is not uniformly true,
    # depending on use case, but can be adjusted as needed.
    subset = base[base["LOWER_VAL"] <= 400]
    return subset


# Primary function for determining legality of each flight point
# Params:
#   rel_restrictions: geodataframe representing restricted airspace relevant to our search
#   test_locs: geodataframe of points naively representing flights
def analyze_flights(rel_restrictions, test_locs):
    # Results will be stored in a pandas dataframe for simple management and export
    results_df = pd.DataFrame(
        columns=['Location_FID', 'Location_Lat', 'Location_Long', 'Legality', 'Airspace_Class', 'Airspace_Name',
                 'Airport_ID'])

    for index, location in test_locs.iterrows():
        # Retrieve the geometry for the current feature by dynamically accessing the geometry column name
        loc_geom = location[test_locs.geometry.name]
        # Select by location to access restricted airspace polygons that contain the relevant point
        conflicts = rel_restrictions[rel_restrictions.contains(loc_geom)]

        # All points, legal or not, will be represented with their FID and lat/long
        new_row = {
            'Location_FID': index,
            'Location_Lat': loc_geom.y,
            'Location_Long': loc_geom.x
        }

        # Illegal flights will include the class of restricted airspace, name of the airspace restriction, and
        # identifier for the nearest airport
        if not conflicts.empty:
            new_row['Legality'] = 'Illegal'
            new_row['Airspace_Class'] = conflicts['CLASS'].values[0]
            new_row['Airspace_Name'] = conflicts['NAME'].values[0]
            new_row['Airport_ID'] = conflicts['IDENT'].values[0]
        # Flag legal flights
        else:
            new_row['Legality'] = 'Legal'

        results_df = results_df.append([new_row])

    return results_df


# Main program access
def main():
    # Access configurable parameters
    args = get_arg_dict()

    # Load and filter restricted airspace file
    restrictions = load_nasr_shapefile(args['date'], force_file=args['queryFile'])
    relevant_restrictions = apply_relevancy_filters(restrictions)

    # Load our flight locations
    test_locations = geopandas.read_file("zip://{}".format(args['locations']))

    # Determine legal status of each flight, and write the result to a csv
    results = analyze_flights(relevant_restrictions, test_locations)
    results.to_csv(args['outFile'], index=False)


main()
