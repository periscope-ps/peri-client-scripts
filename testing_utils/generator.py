#!/usr/bin/env python
"""
Generate three levels tree topology.
"""
import json

__author__ = "Ahmed El-Hassany <a.hassany@gmail.com>"


PORT = "http://unis.incntre.iu.edu/schema/20120709/port#"
NODE = "http://unis.incntre.iu.edu/schema/20120709/node#"
NETWORK = "http://unis.incntre.iu.edu/schema/20120709/network#"

LINK = "http://unis.incntre.iu.edu/schema/20120709/link#"

def gen_port(name, address):
    port = {"$schema": PORT}
    port['address'] = address
    port['name'] = name
    port['id'] = name
    return port


def get_mac(int_addr):
    txt = "%012x" % int_addr
    mac = ""
    mac = txt[0:2] + ":" + txt[2:4] + ":" + txt[4:6] + ":" + txt[6:8] + ":" + txt[8:10] + ":" + txt[10:12]
    return mac

def gen_node(name, ports):
    node = {'$schema': NODE}
    node['id'] = name
    node['name'] = name
    node['ports'] = ports
    return node

def gen_link(name, directed, endpoints):
    link = {'$schema': LINK}
    link['id'] = name
    link['name'] = name
    link['directed'] = directed
    if directed == True:
        link['endpoints'] = {'source': endpoints[0], 'sink': endpoints[1]}
    else:
        link['endpoints'] = endpoints
    return link


def gen_network(name, hosts_per_switch, switches_per_agg, ncores):
    net = {'$schema': NETWORK}
    net['id'] = name
    net['name'] = name
    net['ports'] = []
    net['nodes'] = []
    net['links'] = []
    
    aaports = []
    for a in range(ncores):
        aports = []
        for s in range(switches_per_agg):
            sports = []
            for n in range(hosts_per_switch):
                sport = gen_port("a%d_s%d_n%d_eth0" % (a, s, n), {'address': get_mac(s << 10 | n), 'type': 'mac'})
                port = gen_port("a%s_s%d_n%d_eth0" % (a, s, n), {'address': get_mac(n), 'type': 'mac'})
                net['ports'].append(sport)
                hsport = {'href': '#/ports/%d' % (len(net['ports']) - 1), 'rel': 'full'}
                sports.append(hsport)
                net['ports'].append(port)
                hport = {'href': '#/ports/%d' % (len(net['ports']) - 1), 'rel': 'full'}
                node = gen_node("a%d_s%d_n%d" % (a, s, n), [hport])
                net['nodes'].append(node)
                link = gen_link("a%d_s%d_n%d" % (a, s, n), False, [hsport, hport])
                net['links'].append(link)
            saport = gen_port("s%d_a%d_eth0" % (s, a), {'address': get_mac(a << 20 | s), 'type': 'mac'})
            net['ports'].append(saport)
            hsaport = {'href': '#/ports/%d' % (len(net['ports']) - 1), 'rel': 'full'}
            sports.append(hsaport)

            asport = gen_port("a%d_s%d_eth0" % (a, s), {'address': get_mac(a << 21 | s), 'type': 'mac'})
            net['ports'].append(asport)
            hasport = {'href': '#/ports/%d' % (len(net['ports']) - 1), 'rel': 'full'}
            aports.append(hasport)
            
            link = gen_link("a%d_s%d" % (a, s), False, [hasport, hsaport])

            net['links'].append(link)
            switch = gen_node('s%d' % s, sports)
            net['nodes'].append(switch)
        # adding extra ports to connect to other core switches
        ncoreports = []
        for i in range(ncores):
            nport =  gen_port("a%d_s%d_p0" % (a, s), {'address': get_mac(a << 15 | i), 'type': 'mac'})
            net['ports'].append(nport)
            hnport = {'href': '#/ports/%d' % (len(net['ports']) - 1), 'rel': 'full'}
            ncoreports.append(hnport)
        aaports.append(ncoreports)
        agg = gen_node('a%d' % a, aports)
        net['nodes'].append(agg)
    
    for i in range(len(aaports)):
        ncoreport1 = aaports[i]
        for j in range(len(aaports)):
            ncoreport2 = aaports[j]
            if i == j:
                continue
            for k in range(len(ncoreport1)):
                port1 = ncoreport1[k]
                for l in range(len(ncoreport2)):
                   port2 = ncoreport2[l]
                   link = gen_link("core_%d_%d_to_%d_%d" % (i, k, j, l), False, [port1, port2])
                   net['links'].append(link)
            
    return net



def main():
    net = gen_network('test', 32, 16, 8)
    print json.dumps(net, indent=2)

if __name__ == '__main__':
    main()


    
