#########################################################################
#                                                                       #
# Restore Elastic Search Indices v1.0 Feb 2021                          #
#                                                                       #
# by Muzo                                                        #
#                                                                       #
# Demo code to restore the indices of a Standalone Elasticsearch Node   #
# Check the logfile /tmp/restorations.log for the errors                #
#                                                                       #
# ES ip,port and the repository should be specified at es.properties    #
#                                                                       #
# ie                                                                    #
#                                                                       #
# es_address=124.252.253.124                                            #
# port=9200                                                             #
# repo=backup                                                           #
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
  logging.basicConfig(filename='/tmp/restorations.log',level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
  read_properties()
  es=ES([esa])
  sn=input("snapshot to be restored: ")
  logging.info("API sent to restore the snapshot %s"%(sn))
  stat=""
  while stat != "True":
    try:
      res=es.snapshot.restore(repository='backup', snapshot='muzo22')
      res=str(res)
      stat=res[13:17]

    except Exception as e:
      s=str(e)
      i=s.find("cannot restore index")
      s=s[i:]
      st=s[s.find('[')+1:s.find(']')]
      print("Closing the open index:",st)
      es.indices.close(index=st)
      logging.info("closing the open index: %s"%(st))      
 
  print("Final Status of the Restoration: ",res)
  logging.info("Final Status of the Restoration: %si"%(res))
  
  print("Please wait 15 secs for the indices to recover")
  time.sleep(15)
  res=es.cat.indices()
  print(res)
  logging.info(res)
  
  

if __name__ == "__main__":
 main()

