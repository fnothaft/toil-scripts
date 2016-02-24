# Audrey Musselman-Brown
#

import time
import boto.sdb
import boto.exception
import threading
from collections import OrderedDict
import sys

class Samples(object):

    def __init__(self, conn, dom, version, samples):
        self.conn = conn
        self.dom = dom
        self.version = version
        self.samples = samples

    @classmethod
    def load(cls, conn, dom):
        nodes_per_sample = dom.get_attributes("nodes_per_sample")
        if 'version' in nodes_per_sample:
            version = int(nodes_per_sample.pop('version'))
        else:
            #dom.put_attributes("nodes_per_sample", {'version': str(0)})
            version = 0
        samples = OrderedDict(map(lambda x:[x[0], map(int, x[1].split(','))],
                                  sorted(nodes_per_sample.items(),
                                         key=(lambda x: x[1])))) 
        return cls(conn, dom, version, samples)
    
    def update(self):
        nodes_per_sample = self.dom.get_attributes("nodes_per_sample")
        if 'version' in nodes_per_sample:
            self.version = int(nodes_per_sample.pop('version'))
        else:
            dom.put_attributes("nodes_per_sample", {'version': str(0)})
            self.version = 0
        self.samples = OrderedDict(map(lambda x:[x[0], map(int, x[1].split(','))],
                                       sorted(nodes_per_sample.items(),
                                              key=(lambda x: x[1]))))

    def reorder(self):
        self.samples = OrderedDict(sorted(self.samples.items(),
                                          key=(lambda x: x[1])))
        
    def save(self):
        attributes = dict({key: "{0},{1}".format(value[0], value[1])
                           for key, value in self.samples.items()},
                           version=str(self.version + 1))
        try:
            self.dom.put_attributes("nodes_per_sample", attributes,
                                    expected_value=('version', str(self.version)))
            self.reorder()
            return True
        except boto.exception.SDBResponseError as e:
            if e.error_code == 'ConditionalCheckFailed':
                return False
            else:
                raise e
    
    def increase_nodes(self, sampleID, n):
        """
        Increases the node count for sampleID by n and returns once the nodes
        become available
        """
        while True:
            self.update()
            if sampleID in self.samples:
                self.samples[sampleID][0] = self.version+1
                self.samples[sampleID][1] += n
                
            else:
                self.samples[sampleID] = [self.version+1, n]
            if self.save():
                break
        cluster_size = ClusterSize.load(conn, dom)
        while True:
            print "waiting for nodes to be allocated"
            print "samples are ", self.samples
            print "cluster_size is ", cluster_size.size
            sum = 0
            for sample, [sample_version, nodes] in self.samples.items():
                sum += nodes
                if sample == sampleID and sum <= cluster_size.size:    
                    return
                if sum > cluster_size:
                    break
            self.update()
            cluster_size.update()
            time.sleep(60)

    def decrease_nodes(self, sampleID, n):
        """
        Decreases the node count for sampleID by n
        """
        while True:
            self.update()
            self.samples[sampleID][1] -= n
            self.samples[sampleID][1] = min(self.samples[sampleID], 0)
            if self.save():
                break


class ClusterSize(object):

    def __init__(self, conn, dom, version, size):
        self.conn = conn
        self.dom = dom
        self.version = version
        self.size = size

    @classmethod
    def load(cls, conn, dom):
        cluster_size = dom.get_attributes("cluster_size")
        if 'version' in cluster_size:
            version = int(cluster_size['version'])
        else:
            dom.put_attributes("cluster_size", {'version': str(0)})
            version = 0
        if 'size' in cluster_size:
            size = int(cluster_size['size'])
        else:
            dom.put_attributes("cluster_size", {'size': str(0)})
            size = 0
        return cls(conn, dom, version, size)

    def update(self):
        cluster_size = self.dom.get_attributes("cluster_size")
        if 'version' in cluster_size:
            self.version = int(cluster_size['version'])
        else:
            self.dom.put_attributes("cluster_size", {'version': str(0)})
            self.version = 0
        if 'size' in cluster_size:
            self.size = int(cluster_size['size'])
        else:
            self.dom.put_attributes("cluster_size", {'size': str(0)})
            self.size = 0

    def save(self):
        attributes = dict(size=str(self.size), version=str(self.version+1))
        try:
            self.dom.put_attributes("cluster_size", attributes, expected_value=('version', str(self.version)))
            return True
        except boto.exception.SDBResponseError as e:
            if e.error_code == 'ConditionalCheckFailed':
                return False
            else:
                raise

    def change_size(self, n):
        while True:
            self.update()
            self.size = n
            if self.save():
                break


if __name__=="__main__":
    print "start"
    conn = boto.sdb.connect_to_region('us-west-2', aws_access_key_id="AKIAJQLD3VAM2AZOTBXA",
                                      aws_secret_access_key="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")
    print "conn success"
    dom = conn.create_domain('test_domain')
    print "dom success"
    
    dom.delete_attributes("cluster_size")
    dom.delete_attributes("nodes_per_sample")
    print "attributes deleted"

    samples_and_nodes = Samples.load(conn, dom)
    cluster_size = ClusterSize.load(conn, dom)
    print "samples and cluster size loaded"
    print "initial nodes", samples_and_nodes.samples, samples_and_nodes.version
    print "initial cluster size", cluster_size.size, cluster_size.version
   
    print "~~~~~~~~~~~~~~increase  sample 2 nodes by 1~~~~~~~~~~~~~~~~~~~~"
    samples_and_nodes.increase_nodes("2", 1)
    samples_and_nodes.update()
    print "updated:", samples_and_nodes.samples, samples_and_nodes.version
    
    print "~~~~~~~~~~~~~~increase  sample 1 nodes by 1~~~~~~~~~~~~~~~~~~~~"
    samples_and_nodes.increase_nodes("1", 1)
    samples_and_nodes.update()
    print "updated:", samples_and_nodes.samples, samples_and_nodes.version
    
    print "~~~~~~~~~~~~~~increase  sample 3 nodes by 1~~~~~~~~~~~~~~~~~~~~"
    samples_and_nodes.increase_nodes("3", 1)
    samples_and_nodes.update()
    print "updated:", samples_and_nodes.samples, samples_and_nodes.version
    
    print "~~~~~~~~~~~~~~increase  sample 1 nodes by 2~~~~~~~~~~~~~~~~~~~~"
    samples_and_nodes.increase_nodes("1", 2)
    samples_and_nodes.update()
    print "updated:", samples_and_nodes.samples, samples_and_nodes.version
    
    print "~~~~~~~~~~~~~~increase  sample 2 nodes by 3~~~~~~~~~~~~~~~~~~~~"
    samples_and_nodes.increase_nodes("2", 3)
    samples_and_nodes.update()
    print samples_and_nodes.samples, samples_and_nodes.version
    
    print "~~~~~~~~~~~~~~decrease  sample 1 nodes by 1~~~~~~~~~~~~~~~~~~~~"
    samples_and_nodes.decrease_nodes("1", 1)
    samples_and_nodes.update()
    print samples_and_nodes.samples, samples_and_nodes.version
    
    print "~~~~~~~~~~~~~~decrease  sample 1 nodes by 1~~~~~~~~~~~~~~~~~~~~"
    samples_and_nodes.decrease_nodes("1", 1)
    samples_and_nodes.update()
    print samples_and_nodes.samples, samples_and_nodes.version
    sys.exit()
