import requests
import zipfile
import datetime
import sys
import Jablonski_CLI_Arg_Handler as arg_handler


def retrieveNasrShapefiles(date):
    result = requests.get("https://nfdc.faa.gov/webContent/28DaySub/28DaySubscription_Effective_2021-06-17.zip")
    open('reqs.zip', 'wb').write(result.content)


def buildDateFromStr(dtString):
    try:
        year, month, day = dtString.split('-')
        return datetime.date(year, month, day)
    except ValueError:
        print("Illegal date format.  Must be in YYYY-MM-DD format.")
        sys.exit()


def getArgDict():
    default_args = {
        'date': datetime.date.today(),
        'queryFile': None
    }

    required_args = []

    arg_dict, rcvd = arg_handler.retrieve_args(defaults=default_args, required=required_args)
    
    if 'date' in rcvd:
        try:
            arg_dict['date'] = datetime.datetime.fromisoformat(arg_dict['date'])
        except ValueError:
            print("Illegal date format.  Must be in format YYYY-MM-DD")
            sys.exit()
    return arg_dict


args = getArgDict()

#retrieveNASR(args['date'])
