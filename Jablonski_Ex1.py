import requests
import datetime
import sys
import os
import geopandas
import Jablonski_CLI_Arg_Handler as arg_handler


def loadNasrShapefile(date, nasrpath="./nasr/"):
    filepath = os.path.join(nasrpath, "{}.zip".format(date))
    if not os.path.exists(filepath):
        retrieveNasrShapefiles(date, nasrpath)
    gdf = geopandas.read_file("zip://{}!Shape_Files".format(filepath))
    print(gdf.columns)
    return gdf


def retrieveNasrShapefiles(date, dlpath="./nasr/"):
    print("downloading")
    remote = "https://nfdc.faa.gov/webContent/28DaySub/{}/class_airspace_shape_files.zip".format(str(date))
    result = requests.get(remote)
    if not result.status_code == 200:
        print("Error in file download.\nUrl used: {}\nRemote returned code: {}".format(remote, result.status_code))
        sys.exit()

    if not os.path.exists(dlpath):
        os.makedirs(dlpath)

    with open("{}{}.zip".format(dlpath, date), 'wb') as outFile:
        outFile.write(result.content)


def getArgDict():
    default_args = {
        'date': datetime.date.today(),
        'queryFile': None
    }

    required_args = []

    arg_dict, rcvd = arg_handler.retrieve_args(defaults=default_args, required=required_args)
    
    if 'date' in rcvd:
        try:
            arg_dict['date'] = datetime.datetime.strptime(arg_dict['date'], "%Y-%m-%d").date()
        except ValueError:
            print("Illegal date format.  Must be in format YYYY-MM-DD")
            sys.exit()
    return arg_dict


args = getArgDict()

loadNasrShapefile(args['date'])
