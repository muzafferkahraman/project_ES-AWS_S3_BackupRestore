#########################################################################
#									#
# Snapshot Elastic Search Indices v1.0 Feb 2021				#
#									#
# by Muzo							#
#									#
# Demo code to snapshot the indices of a Standalone Elasticsearch Node  #
# Check the logfile /tmp/snapshots.log for the errors			#
#									#
# ES ip,port and the repository should be specified at es.properties    #
#									#
# ie									#	
#									#	
# es_address=124.252.253.124						#
# port=9200								#
# repo=backup								#	
#########################################################################

def read_properties():
 
  global esa
  global repo
  with open("es.properties","r") as f:
    ip=(f.readline()).partition("=")[2].strip()
    port=(f.readline()).partition("=")[2].strip()
    repo=str(f.readline()).partition("=")[2].strip()
    esa=ip+":"+port


def main():

  from elasticsearch import Elasticsearch as ES
  import logging
  import time
  global es
  global sn  
  logging.basicConfig(filename='/tmp/snapshots.log',level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
  read_properties()
  es=ES([esa])
  sn=input("snapshotname: ")
  es.snapshot.create(repository=repo, snapshot=sn)
  logging.info("API sent to create snapshot %s"%(sn))
  print("checking the final status of the snapshot - please wait")
  status="STARTED"
  while status=="STARTED":
    res=es.snapshot.status(repository=repo, snapshot=sn)
    status=res['snapshots'][0]['state']
    time.sleep(2)
    if status != "STARTED":
      logging.info(res)
      break
  print("result:",res['snapshots'][0]['state'])
  logging.info("Snapshot Status %s"%(res['snapshots'][0]['state']))

if __name__ == "__main__":
 main()

