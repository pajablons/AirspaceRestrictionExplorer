import sys

def _unpack(legalKeys, arg_arr, dropKeyCase, deliminator):
    keypairs = {}
    for arg in arg_arr:
        try:
            key, value = arg.split(deliminator)
            if dropKeyCase:
                key = key.lower()
            if not key in legalKeys:
                raise NameError(key)
            keypairs[key] = value
        except NameError as err:
            print("Illegal argument provided: {}".format(str(err.args[0])))
        except ValueError:
            print("Argument syntax error.  Please check your provided arguments.")
            sys.exit()
    return keypairs

def retrieve_args(required = [], defaults = {}, dropKeyCase = False, pairwise_delim = '='):
    arg_arr = sys.argv[1:]
    
    arg_dict = _unpack(defaults.keys(), arg_arr, dropKeyCase, pairwise_delim)

    if not all(key in arg_dict for key in required):
        print("Missing required arguments.  Exiting.")
        sys.exit()
    
    ret = defaults
    ret.update(arg_dict)
    return ret, arg_dict.keys()