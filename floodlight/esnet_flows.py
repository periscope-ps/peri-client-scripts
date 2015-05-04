#!/usr/bin/python

import sys
import argparse
import httplib
import json

DPID_LBL = "00:00:00:00:00:00:00:18"
DPID_NER = "00:00:00:00:00:00:00:0a"
DPID_BNL = "00:00:00:00:00:00:00:03"
DPID_ANL = "00:00:00:00:00:00:00:02"

node = {}

node['nersc'] = {
    'port_a': '25',
    'port_z': '26',
    'switch': DPID_NER}    

node['lbl'] = {
    'port_a': '17',
    'port_z': '18',
    'switch': DPID_LBL}

node['anl'] = {
    'port_a': '25',
    'port_z': '26',
    'switch': DPID_ANL}

paths = {}

paths['2hop'] = [{'hop':'lbl', 'vlan_a':'3291', 'vlan_z':'3291'},
                 {'hop':'nersc', 'vlan_a':'3291', 'vlan_z':'3292'},
                 {'hop':'anl', 'vlan_a':'3291', 'vlan_z':'3292'}]

paths['1hop'] = [{'hop':'lbl', 'vlan_a':'3291', 'vlan_z':'3293'},
                 {'hop':'anl', 'vlan_a':'3291', 'vlan_z':'3293'}]

class RestApi(object):

    def __init__(self, server, port):
        self.server = server
        self.port = port


    def get(self, data):
        ret = self.rest_call({}, 'GET')
        return json.loads(ret[2])

    def set(self, data):
        ret = self.rest_call(data, 'POST')
        return ret[0] == 200

    def remove(self, data):
        ret = self.rest_call(data, 'DELETE')
        return ret[0] == 200

    def rest_call(self, data, action):
        path = '/wm/staticflowentrypusher/json'
        headers = {
            'Content-type': 'application/json',
            'Accept': 'application/json',
            }
        body = json.dumps(data)
        conn = httplib.HTTPConnection(self.server, self.port)
        conn.request(action, path, body, headers)
        response = conn.getresponse()
        ret = (response.status, response.reason, response.read())
        print ret
        conn.close()
        return ret

def make_direct_flow(dpid, entry, name, vlan):
    return {'switch': dpid,
            'name': 'direct-'+str(name),
            'active': 'true',
            'vlan-id': vlan,
            'dst-mac': entry['mac'],
            'actions': 'output='+str(entry['port'])
            }

def make_rewrite_flow(dpid, smac, inp, outp, inv, outv, name):
    return {'switch': dpid,
            'name': 'mod-'+str(name),
            'active': 'true',
            'vlan-id': inv,
            'ingress-port': inp,
            #'src-mac': smac,
            'priority': 1000,
            'actions': 'set-vlan-id='+str(outv)+',output='+str(outp)
            }

def make_path_flows(path):
    pflows = []
    for h in path:
        n = node[h['hop']]
        name = h['hop']+'-'+n['port_a']+'-'+h['vlan_a']+'-'+n['port_z']+'-'+h['vlan_z']
        pflows.append(make_rewrite_flow(n['switch'], None, n['port_a'], n['port_z'], h['vlan_a'], h['vlan_z'], name))
        name = h['hop']+'-'+n['port_z']+'-'+h['vlan_z']+'-'+n['port_z']+'-'+h['vlan_a']
        pflows.append(make_rewrite_flow(n['switch'], None, n['port_z'], n['port_a'], h['vlan_z'], h['vlan_a'], name))
    return pflows

usage_desc = """
test_flow.py {add|del} [path id] ...
"""

parser = argparse.ArgumentParser(description='process args', usage=usage_desc, epilog='foo bar help')
parser.add_argument('--ip', default='localhost')
parser.add_argument('--port', default=9000)
parser.add_argument('cmd')
parser.add_argument('path', nargs='?', default=None)
parser.add_argument('otherargs', nargs='*')
args = parser.parse_args()

#print "Called with:", args
cmd = args.cmd

# handle to Floodlight REST API
rest = RestApi(args.ip, args.port)

flows = []

if args.path in ['1hop', '2hop']:
    flows.extend(make_path_flows(paths[args.path]))
else:
    print "Unknown path"
    exit

for f in flows:
    if (cmd=='add'):
        print "Adding flow:", f
        rest.set(f)
    if (cmd=='del'):
        print "Deleting flow:", f
        rest.remove(f)
