#!/usr/bin/env python
"""
A simple script to pull the RSpecs from aggregate managers.
"""

__author__ = "Ahmed El-Hassany <a.hassany@gmail.com>"
__version__ = "0.1dev"

import argparse
import ConfigParser
import logging
import itertools
import multiprocessing
import subprocess
import tempfile
import os.path


# Setting basic logging
log = logging.getLogger('am_pull')
log.setLevel(logging.DEBUG)

# The location of Omni script
DEFAULT_OMNI = "~/workdir/geni/gcf-2.0/src/omni.py"
# The location of Omni configuation file
DEFAULT_OMNI_CONF = "~/.gcf/omni_config"

# The location of the unisencoder
DEFAULT_UNISENCODER = "unisencoder"

DEFAULT_UNIS_URL = "http://129.79.244.8:8888"

CHUNKSIZE = 4

def pull_aggregate_manager(urn, url, omni, omni_conf):
    """
    Pulls the advertisment RSpec from one aggregate manager.
    
    Params:
        urn: the URN of the aggregate manager.
        url: the URL of the aggregate manager.
        omni: the path for OMNI executable
        omni: the path for omni configuration file.
    
    Returns:
        a tuple of urn and the name of rspec temp file or error code.
    """
    log.info("Pulling aggergate manager %s: %s with omni conf %s %s" % \
        (urn, url, omni, omni_conf))
    f = tempfile.NamedTemporaryFile(delete=False)
    ret = subprocess.call("%s -c %s -a %s listresources --outputfile=%s" % \
        (omni, omni_conf, url, f.name), shell=True)
    if ret == 0:
        return (urn, f.name)
    else:
        return ret

def pull_aggregate_manager_wrapper(input):
    """
    Partial Functions doesn't work with multiprocessing. This is a work arround.
    
    Params:
       input in the form of (('urn', 'aggregate_url'), (omni exec, omni conf))
    
    Retuens:
        same as pull_aggregate_manager
    """
    return pull_aggregate_manager(input[0][0], input[0][1],
        input[1][0], input[1][1])

def pull_aggregate_managers(aggregate_managers, omni, omni_conf):
    """
    Pulls the advertisment rspecs from the aggregate managers and 
    save them to temp files.

    Params:
      aggregate_mangagers: a dict of aggregate managers URN and URL
    
    Returns:
      a dict of aggregate managers URNs and the name of rspec temp files.
    """
    log.info("Start pulling from all aggregate managers")
    output_files = {}
    pool = multiprocessing.Pool(None)
    tasks = itertools.izip(aggregate_managers.items(),
                           itertools.repeat((omni, omni_conf)))
    for x in pool.imap(pull_aggregate_manager_wrapper, tasks, chunksize=CHUNKSIZE):
        if isinstance(x, tuple):
            output_files[x[0]] = x[1]
        else:
            return x
    pool.close()
    return output_files
    

def encode_rspec_to_unis(urn, rspec_filename, unisecnoder):
    """
    Reads an rspec from a file and produce a UNIS file

    Params:
      urn: the URN of the aggregate manager that produced the rspec
      rspec_file_name: the  full name of the rspec

    Returns:
       (urn, unis_file_name) 
    """
    log.info("UNIS encoding of %s from file %s" % (urn, rspec_filename))
    f = tempfile.NamedTemporaryFile(delete=False)
    print "Invoking", "%s -t rspec3 -m %s -o %s %s" % \
         (unisecnoder, urn, f.name, rspec_filename)
    print "Params", "unisencoder", unisecnoder, "urn", urn, rspec_filename
    ret = subprocess.call("%s -t rspec3 -m %s -o %s %s" % \
        (unisecnoder, urn, f.name, rspec_filename), shell=True)
    if ret == 0:
        return (urn, f.name)
    else:
        return ret

def encode_rspec_wrapper(input):
    """
    This a work around to make multiprocessing work with more than one variable.
    """
    print "Encode rspec wrapper", input
    return encode_rspec_to_unis(input[0][0], input[0][1], input[1])
    
def encode_rspecs_to_unis(rspecs, unisencoder):
    """
    Invokes UNISENCODER to encode rspecs to UNIS format.

    Params:
       rspecs: a dict of rspecs URNs and full filename

    Returns:
      a dict of URNs and the name of UNIS temp files
    """
    log.info("Encoding all rspecs to UNIS")
    output_files = {}
    pool = multiprocessing.Pool(None)
    tasks = itertools.izip(rspecs.items(),
                           itertools.repeat(unisencoder))
    for x in pool.imap(encode_rspec_wrapper, tasks, chunksize=CHUNKSIZE):
        if isinstance(x, tuple):
            output_files[x[0]] = x[1]
        else:
            return x
    pool.close()
    return output_files
    

def send_to_unis(unis_files, unis_url):
    """
    HTTP POST to the UNIS Instance.
    
    Params:
        unis_files: index of URN and name of the file to be sent
        unis_url: the UNIS instance 
    
    Returns:
        None
    """
    for f in unis_files.values():
        log.info("Sending %s to UNIS %s" % (f, unis_url))
        ret = subprocess.call(
            'curl -X POST -H "Content-Type: application/perfsonar+json" -d@%s %s' % \
            (f, unis_url + "/domains"),
            shell=True)
        if ret != 0:
            return ret
        

def is_valid_file(parser, arg, flag='r', mode=0666):
    if flag.startswith('r'):
        if not os.path.exists(arg):
            parser.error("The file %s does not exist!"%arg)
    return open(arg, flag, mode)


def main():
    # Configuration's sections names
    AM_SEC = 'Aggregate_Managers'
    OMNI_SEC = 'OMNI'
    UNIS_SEC = 'UNIS'
    UNISENCODER_SEC = 'UNISENCODER'
    
    # User input
    parser = argparse.ArgumentParser(
        description="Pulls topologies from perfSONAR Topology Services"
        "then encode them to UNIS format and pushes it to UNIS service"
    )
    parser.add_argument('-c', '--config',
        type=lambda x: is_valid_file(parser,x, 'r'))
    parser.add_argument('-m', '--aggregate_manager', type=str, default=None,
        help='The URL of the aggregate manager.')
    parser.add_argument('--urn', type=str, default=None,
        help='The URM of the aggregate manager.')
    parser.add_argument('-e', '--encoder', type=str, default=None,
        help='The unisencoder executable.')
    parser.add_argument('-u', '--unis_url', type=str, default=None,
        help='The URL of the UNIS instance')
    parser.add_argument('--omni', type=str, default=None,
        help='OMNI executable')
    parser.add_argument('--omni_conf', type=str, default=None,
        help='OMNI configuration')
    parser.add_argument('-l', '--log',
        type=lambda x: is_valid_file(parser,x, 'w'),
        help='Log file.')
    args = parser.parse_args()
    
    formatter = logging.Formatter('[%(levelname)s] %(message)s')
    if args.log is None:
        handler_stream = logging.StreamHandler()
        log.addHandler(handler_stream)
    else:
        print "should write to log file"
    
    # Assume the default values first
    unis_url = DEFAULT_UNIS_URL
    unisencoder = DEFAULT_UNISENCODER
    omni = DEFAULT_OMNI
    omni_conf = DEFAULT_OMNI_CONF
    
    if args.config is not None:
        # Read configurarion
        config = ConfigParser.ConfigParser()
        config.readfp(args.config)
        
        # Make sure at least something need to be pulled
        if not config.has_section(AM_SEC):
            raise Exception("No Aggregate_Managers are defined in "
                            "the configuration file")
        aggregate_managers = {}
        if config.has_section(AM_SEC):
            for urn, url in config.items(AM_SEC):
                aggregate_managers[urn] = url
        unis_url = config.get(UNIS_SEC,'url') \
            if config.has_option(UNIS_SEC, 'url') \
            else unis_url
        unisencoder = config.get(UNISENCODER_SEC,'exec') \
            if config.has_option(UNISENCODER_SEC, 'exec') \
            else unisencoder
        omni = config.get(OMNI_SEC, 'exec') \
            if config.has_option(OMNI_SEC, 'exec') \
            else omni
        omni_conf = config.get(OMNI_SEC, 'conf') \
            if config.has_option(OMNI_SEC, 'conf') \
            else omni_conf
    elif args.aggregate_manager is not None and args.urn is not None:
        aggregate_managers = {}
        aggregate_managers[args.urn] = [args.aggregate_manager]
    else:
        parser.error("Either a configuration file or a "
            "aggregate manager should be provided")
    
    # Read command line arguments
    # Command line arguments overrides values on a configuration file
    unis_url = args.unis_url or unis_url
    unisencoder = args.encoder or unisencoder
    omni = args.omni or omni
    omni_conf = args.omni_conf or omni_conf
    
    # Start pulling rspecs from aggregate managers
    rspecs = pull_aggregate_managers(aggregate_managers, omni, omni_conf)
    if not isinstance(rspecs, dict):
        return rspecs
    unis_files = encode_rspecs_to_unis(rspecs, unisencoder)
    if not isinstance(unis_files, dict):
        return unis_files
    send_to_unis(unis_files, unis_url)

    # Delete temp files
    for urn, f in rspecs.iteritems():
        os.remove(f)
    for urn, f in unis_files.iteritems():
        os.remove(f)


if __name__ == '__main__':
    main()
