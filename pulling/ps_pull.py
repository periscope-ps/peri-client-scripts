#!/usr/bin/env python
"""
A simple script to pull the topologies from perfSONAR Topology services.
"""

__author__ = "Ahmed El-Hassany <a.hassany@gmail.com>"
__version__ = "0.1dev"

import argparse
import ConfigParser
from lxml import etree
import logging
import itertools
import multiprocessing
import subprocess
import tempfile
import os.path
import urllib2

# Setting basic logging
log = logging.getLogger('ps_pull')
log.setLevel(logging.DEBUG)

# The location of the unisencoder
DEFAULT_UNISENCODER = "unisencoder"

DEFAULT_UNIS_URL = "http://129.79.244.8:8888"

NMTOPO = "http://ogf.org/schema/network/topology/base/20070828/"

CHUNKSIZE = 4


# Configuration's sections names
PS_SEC = 'perfSONAR_Topologies'
UNIS_SEC = 'UNIS'
UNISENCODER_SEC = 'UNISENCODER'

def make_envelope(content):
    """
    Makes SOAP protocl envolope.
    """
    envelope = """
    <SOAP-ENV:Envelope xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/"
        xmlns:xsd="http://www.w3.org/2001/XMLSchema"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/">
    <SOAP-ENV:Header/>
    <SOAP-ENV:Body>
        %s
    </SOAP-ENV:Body>
    </SOAP-ENV:Envelope>
    """ % content
    return envelope

def send_receive(url, envelope):
    """
    Communicate with web service.
    """
    req = urllib2.Request(url=url, data=envelope,
        headers={
            'Content-type': 'text/xml; charset="UTF-8"',
            'SOAPAction': 'http://ggf.org/ns/nmwg/base/2.0/message/'
        }
    )
    file_handler = urllib2.urlopen(req)
    return file_handler.read()


def pull_topology(name_url):
    """
    Pulls a topology from perfSONAR Topology Service.
    
    Params:
        name_url: a tuple of URN and accesspoint
    """
    log.info("Pulling topology from %s: %s" % (name_url[0], name_url[1]))
    query = """
    <nmwg:message type="TSQueryRequest" id="msg1"
    xmlns:nmwg="http://ggf.org/ns/nmwg/base/2.0/"
    xmlns:xquery="http://ggf.org/ns/nmwg/tools/org/perfsonar/service/lookup/xquery/1.0/">
    <nmwg:metadata id="meta1">
    <nmwg:eventType>http://ggf.org/ns/nmwg/topology/20070809</nmwg:eventType>
    </nmwg:metadata>
    <nmwg:data metadataIdRef="meta1" id="d1" />
    </nmwg:message>
    """
    envelope = make_envelope(query)
    return (name_url[0], send_receive(name_url[1], envelope))

def pull_topologies(services):
    """
    Pulling topology information from the given list of services.
    
    Params:
        services: a dict of URN : service accesspoint
    Return:
        a dict of URN: filename; where the file name contains the topology info
        in case of failure the error code is returned
    """
    output_files = {}    
    pool = multiprocessing.Pool(None)
    tasks = list(services.items())
    for ret in pool.imap(pull_topology, tasks):
        if not isinstance(ret, tuple):
            return ret
        name = ret[0]
        response = ret[1]
        tree = etree.fromstring(response)
        topology = tree.find(".//{%s}topology" % NMTOPO)
        if topology is None:
            print "No topology was found for service", name
            log.error("No topology was found for service %s " % name)
        else:
            tmpf = tempfile.NamedTemporaryFile(delete=False)
            tmpf.write(etree.tostring(topology))
            output_files[name] = tmpf.name
    return output_files

def encode_topology_to_unis(urn, filename, unisencoder):
    """
    Invokes UNISENCODER to encode topology to UNIS format.

    Params:
       rspecs: a dict of PS URNs and full filename

    Returns:
      a dict of URNs and the name of UNIS temp files
    """
    log.info("Encoding topology %s from file %s" % (urn, filename))
    tmpf = tempfile.NamedTemporaryFile(delete=False)
    # TODO (AH): Add urn
    ret = subprocess.call("%s -t ps -o %s %s" % \
        (unisencoder, tmpf.name, filename), shell=True)
    if ret == 0:
        return (urn, tmpf.name)
    else:
        return ret


def encode_topology_to_unis_wrapper(params):
    """
    This a work around to make multiprocessing work with more than one variable.
    """
    return encode_topology_to_unis(params[0][0], params[0][1], params[1])


def encode_topologies_to_unis(topologies, unisencoder):
    """
    Invokes UNISENCODER to encode topologies to UNIS format.

    Params:
       topologies: a dict of rspecs URNs and full filename

    Returns:
      a dict of URNs and the name of UNIS temp files
    """
    log.info("Encoding all topologies to UNIS")
    output_files = {}
    pool = multiprocessing.Pool(None)
    tasks = itertools.izip(topologies.items(),
                           itertools.repeat(unisencoder))
    for ret in pool.imap(encode_topology_to_unis_wrapper,
        tasks, chunksize=CHUNKSIZE):
        if isinstance(ret, tuple):
            output_files[ret[0]] = ret[1]
        else:
            return ret
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
    for unisf in unis_files.values():
        log.info("Sending %s to UNIS %s" % (unisf, unis_url))
        subprocess.call(
        'curl -X POST -H "Content-Type: application/perfsonar+json" -d@%s %s' % \
         (unisf, unis_url + "/topologies"),
         shell=True)


def is_valid_file(parser, arg, flag='r', mode=0666):
    """
    Auxilary function for argparser to check a file is valid input.
    """
    if flag.startswith('r'):
        if not os.path.exists(arg):
            parser.error("The file %s does not exist!"%arg)
    return open(arg, flag, mode)


def main():
    """Main Method"""
    
    # Defining command line arguments
    parser = argparse.ArgumentParser(
        description="Pulls topologies from perfSONAR Topology Services"
        "then encode them to UNIS format and pushes it to UNIS service"
    )
    parser.add_argument('-c', '--config',
        type=lambda x: is_valid_file(parser,x, 'r'))
    parser.add_argument('-a', '--psservice_accesspoint', type=str, default=None,
        help='The accesspoints of the perfSONAR Topology services.')
    parser.add_argument('--urn', type=str, default=None,
        help='The URN of the perfSONAR topology service.')
    parser.add_argument('-e', '--encoder', type=str, default=None,
        help='The unisencoder executable.')
    parser.add_argument('-u', '--unis_url', type=str, default=None,
        help='The URL of the UNIS instance')
    parser.add_argument('-l', '--log',
        type=lambda x: is_valid_file(parser,x, 'w'),
        help='Log file.')
    args = parser.parse_args()
    
    formatter = logging.Formatter('[%(levelname)s] %(message)s')
    if args.log is None:
        handler_stream = logging.StreamHandler()
        handler_stream.setFormatter(formatter)
        log.addHandler(handler_stream)
    else:
        file_handler = logging.FileHandler(args.log)
        file_handler.setFormatter(formatter)
        log.addHandler(file_handler)
    
    # Assume the default values first
    unis_url = DEFAULT_UNIS_URL
    unisencoder = DEFAULT_UNISENCODER
    
    if args.config is not None:
        # Read configurarion
        config = ConfigParser.ConfigParser()
        config.readfp(args.config)
        
        # Make sure at least something need to be pulled
        if not config.has_section(PS_SEC):
            raise Exception("No perfSONAR Topology services are defined in "
                            "the configuration file")
        psservices = {}
        if config.has_section(PS_SEC):
            for urn, url in config.items(PS_SEC):
                psservices[urn] = url
        unis_url = config.get(UNIS_SEC,'url') \
            if config.has_option(UNIS_SEC, 'url') \
            else unis_url
        unisencoder = config.get(UNISENCODER_SEC,'exec') \
            if config.has_option(UNISENCODER_SEC, 'exec') \
            else unisencoder
    elif args.psservice_accesspoint is not None and args.urn is not None:
        psservices = {}
        psservices[args.urn] = [args.psservice_accesspoint]
    else:
        parser.error("Either a configuration file or a "
            "perfSONAR Topology Service should be provided")
    
    # Read command line arguments
    # Command line arguments overrides values on a configuration file
    unis_url = args.unis_url or unis_url
    unisencoder = args.encoder or unisencoder
    
    # Start pulling rspecs from aggregate managers
    topologies = pull_topologies(psservices)
    if not isinstance(topologies, dict):
        return topologies
    unis_files = encode_topologies_to_unis(topologies, unisencoder)
    if not isinstance(unis_files, dict):
        return unis_files
    send_to_unis(unis_files, unis_url)
    
    # TODO: clean up temp files

if __name__ == '__main__':
    main()
