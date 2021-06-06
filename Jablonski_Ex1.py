# Author: Peter Jablonski
# GEOG666, Exercise 1
# Summer 2021
# Version: Python 3
# Dynamically retrieves flight restriction data from the FAA website for determining legality of flights
# Usage: python Jablonski_Ex1.py locations=Locations.zip [date=YYYY-MM-DD] [outFile=results.csv] [queryFile=PATH_TO_FORCED_FILE]

import requests
import datetime
import sys
import os
import geopandas
import pandas as pd
import zipfile


# Transforms test dates to nasr file subscription dates
# Params:
#   date: A provided datetime object representing a flight date
# Returns: Normalized datetime object matching a nasr file release
def normalize_date_nasr(date):
    # Earliest NASR file is effective 5/23/2019, and subsequent files are released every 28 days.
    MIN_DATE = datetime.date.fromisoformat("2019-05-23")
    NASR_FREQ = 28

    # We can't process any dates before our earliest effective date, so exit on this case
    if date < MIN_DATE:
        print("Illegal date.  Earliest permitted date is {}".format(MIN_DATE))
        sys.exit()

    # Normalize the provided date to a release date (MIN_DATE + 28 * n)
    # First calculate how many days our offset is (how many days it's been since our last subscription file posted)
    offset = (date - MIN_DATE).days % NASR_FREQ
    # Then subtract that offset to get back to the most recent file
    normalized = date - datetime.timedelta(days=offset)
    return normalized


# Primary function for accessing the relevant flight restriction file for a given date, returning a geodataframe
# representation. If the relevant file has not already been downloaded, signals for the file to be retrieved from
# the FAA website.  Defaults to searching in ./nasr/, though this may become command argument configurable.
# If a specific query file is provided, that overrides all date-based file selection and that file is immediately
# retrieved.  This query file must still be in the same format as the downloaded files.
# Params:
#   date: datetime representing flight date for testing
#   nasr_path: Local directory containing flight restriction archives (default: ./nasr/)
#   force_file: Specific local archive to load restrictions from (default: None)
# Returns: Geodatabase containing flight restriction polygons
def load_nasr_shapefile(date, nasr_path="./nasr/", force_file=None):
    norm_date = normalize_date_nasr(date)
    filepath = None
    # First check for and handle if we have a force file
    if force_file:
        filepath = force_file
    else:
        # Craft system path of the form {nasr_path}/{norm_date}.zip
        filepath = os.path.join(nasr_path, "{}.zip".format(norm_date))

        # If the file doesn't already exist, initiate downloading from the remote site
        if not os.path.exists(filepath):
            retrieve_nasr_shapefile(norm_date, dl_path=nasr_path)

    # The zip archive contains a single folder with a single shapefile in it.  Extract this directly into a
    # geodataframe for further analysis
    gdf = geopandas.read_file("zip://{}!Shape_Files".format(filepath))

    # Type correction for future ease of use
    gdf['LOWER_VAL'] = gdf['LOWER_VAL'].astype(int)
    return gdf


# Downloads the flight restriction archive from the FAA website
# Params:
#   date: date to download restrictions for.  Must be a valid release date
#   dl_path: Directory to download restriction archive to
# Returns: None
def retrieve_nasr_shapefile(date, dl_path="./nasr/"):
    compact = True
    # Two possible remote URLs.  We first attempt to use the more compact remote file, and if not reachable,
    # we will download the more comprehensive remote archive
    remote = "https://nfdc.faa.gov/webContent/28DaySub/{}/class_airspace_shape_files.zip".format(str(date))

    # Status check to confirm a successful download
    result = requests.get(remote)
    # 404 error indicates need to use backup file
    if result.status_code == 404:
        remote = "https://nfdc.faa.gov/webContent/28DaySub/28DaySubscription_Effective_{}.zip".format(str(date))
        result = requests.get(remote)
        compact = False

    # If we still haven't succeeded, exit.  Most likely cause is a bad date (too far in the future)
    if not result.status_code == 200:
        print("Error in file download.\nUrl used: {}\nRemote returned code: {}".format(remote, result.status_code))
        sys.exit()

    # Create the directory tree for the archive path, if it does not already exist
    if not os.path.exists(dl_path):
        os.makedirs(dl_path)

    # Write the downloaded binary data to a local zip file.
    # Zip file is in format: {DL_PATH}/{DATE}.zip
    # If we used the non-compact file, file is {DL_PATH}/LF_{DATE}.zip
    outfile_path = os.path.join(dl_path, "{}.zip".format(str(date)))
    with open(outfile_path, 'wb') as out_file:
        out_file.write(result.content)

    # Clean up bloated files so as not to waste disk space.  Extract the one directory we care about and transform it
    # into its own zip archive
    if not compact:
        large_archive = zipfile.ZipFile(outfile_path, 'r')
        small_temp_path = os.path.join(dl_path, "nasr_tmp.zip")
        small_archive = zipfile.ZipFile(small_temp_path, 'w')

        for file in large_archive.infolist():
            if file.filename.startswith('Additional_Data/Shape_Files') and not file.is_dir():
                contents = large_archive.read(file.filename)
                file.filename = file.filename[16:]  # Trim off leading characters to remove the "Additional Data" folder
                small_archive.writestr(file, contents)

        small_archive.close()
        large_archive.close()
        os.remove(outfile_path)
        os.rename(small_temp_path, outfile_path)


# Unpacks user command string into key/value pairs
# Params:
#   legal_keys: List of all allowable parameter keys
#   arg_arr: List of values provided by the user
#   drop_key_case: Treat as case-insensitive (by treating all as lowercase) or not
#   deliminator: String to split keys from values
# Returns: Dictionary of user-supplied keypairs
def _unpack(legal_keys, arg_arr, drop_key_case, deliminator):
    keypairs = {}
    for arg in arg_arr:
        try:
            key, value = arg.split(deliminator)
            if drop_key_case:
                key = key.lower()
            if not key in legal_keys:
                raise NameError(key)
            keypairs[key] = value
        except NameError as err:
            print("Illegal argument provided: {}".format(str(err.args[0])))
        except ValueError:
            print("Argument syntax error.  Please check your provided arguments.")
            sys.exit()
    return keypairs


# Modular function for handling command line arguments.  Ensures receipt of all required arguments,
# and fills in default args.  Allows for varied settings regarding input format.
# Params:
#   required: a list of required parameters (strings)
#   defaults: a dictionary of default pairwise parameters (key: string).  Required or only used if defined -> None
#   drop_key_case: if true, treat all input as uniformly lowercase (effectively makes interpretation case insensitive)
#   pairwise_delim: string delimination between key/value pairs
# Returns: A dictionary of key/value user settings., and a list of what parameters were provided by the user
def retrieve_args(required=[], defaults={}, drop_key_case=False, pairwise_delim='='):
    # Trim the actual program name from the call
    arg_arr = sys.argv[1:]

    # Helper function handles each individual pairing
    arg_dict = _unpack(defaults.keys(), arg_arr, drop_key_case, pairwise_delim)

    # Ensure all required parameters are set
    if not all(key in arg_dict for key in required):
        print("Missing required arguments.  Exiting.")
        sys.exit()

    # Immutably create our final settings from the defaults, then update with provided values
    ret = defaults
    ret.update(arg_dict)
    return ret, arg_dict.keys()


# Establishes initial values for configurable parameters, and parses user provided arguments into appropriate formats
# Params:
#   None
# Returns: Dictionary of parsed user-provided configuration arguments
def get_arg_dict():
    default_args = {
        'date': datetime.date.today(),  # default date is the current date
        'queryFile': None,              # force a specific restriction file (must be same format as downloaded zips)
        'locations': None,              # locations to be tested (point feature class)
        'outFile': 'results.csv'        # file for legality results to be written to (csv)
    }

    required_args = ['locations']       # The only required argument is our input locations

    arg_dict, rcvd = retrieve_args(defaults=default_args, required=required_args)

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
