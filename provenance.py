from elasticsearch import Elasticsearch, helpers
import pymonetdb
from datetime import datetime
import sched, time

class ProvenancePanel:
    def __init__(self, runAll=False, runningInterval=None, msg=False):
        print("Welcome to Provenance Panel!\n")

        if runAll:
            self.connect_elasticsearch(msg)
            self.check_records_elasticsearch(msg)
            self.check_monetdb_new_records(msg)
            self.insert_new_records_elasticsearch(msg)

            if runningInterval:
                self.intervalSecond = runningInterval
                self.run_periodically()

    def run_periodically(self):
        self.s = sched.scheduler(time.time, time.sleep)
        self.s.enter(self.intervalSecond, 1, self.periodicallyFunction)
        self.s.run()

    def periodicallyFunction(self):
        self.checkMonetDB_insertElasticSearch()
        self.s.enter(self.intervalSecond, 1, self.periodicallyFunction)

    def checkMonetDB_insertElasticSearch(self):
        self.check_monetdb_new_records()
        self.insert_new_records_elasticsearch()

    def connect_elasticsearch(self, msg=False):
        if msg: print("---------- ElasticSearch ----------")
        self.index_name = self.set_index_name()
        self.es = Elasticsearch()

    def check_records_elasticsearch(self, msg=False):
        if msg: print("---------- ElasticSearch - Checking records ----------")
        self.last_id_elasticsearch = self.get_last_id_on_elastisearch()

        if msg:
            if self.last_id_elasticsearch == 0:
                print("We checked our index name doesn't exist on ElasticSearch, but we'll create when needed.\n")
            else:
                print(
                    "You have some records in this index and therefore we'll continue from the last ID inserted on ElasticSearch.\n")

    def set_index_name(self):
        index_name = input("Please, type your wished index name or just press enter: ")

        if not index_name:
            index_name = "teste1_ds_otrainingmodel"
            print("No problem! We've set a default one named '%s'.\n" % index_name)
        else:
            print("Perfect! Now, our index name is '%s'\n" % index_name)

        return index_name

    def get_last_id_on_elastisearch(self):
        es = self.es
        index_name = self.index_name
        if es.indices.exists(index=index_name):
            query = \
                {
                    "sort" : [
                        {
                            "doc.id": {
                                "order":"desc"
                            }
                        }
                    ],
                    "size": 1
                }

            res = es.search(index=index_name, body=query)
            return int(res['hits']['hits'][0]['_id']) #last_id_elasticsearch

        else:
            #print("Index não existe!")
            return 0

    def check_monetdb_new_records(self, msg=False):
        es = self.es
        last_id_elasticsearch = self.last_id_elasticsearch
        index_name = self.index_name

        if msg: print("---------- Connecting to MonetDB ----------")
        connection = pymonetdb.connect(username="monetdb_dfa", password="monetdb", hostname="localhost", database="dataflow_analyzer")

        cursor = connection.cursor()
        cursor.arraysize = 100

        if msg: print("Connection OK.\n")

        if msg: print("---------- MonetDB - Checking new records ----------")
        rows = cursor.execute('SELECT ds_otrainingmodel.id, ds_itrainingmodel.optimizer_name, ds_otrainingmodel.adaptation_task_id, ds_otrainingmodel.timestamp, ds_otrainingmodel.elapsed_time, ds_otrainingmodel.loss, ds_otrainingmodel.accuracy, ds_otrainingmodel.val_loss, ds_otrainingmodel.val_accuracy, ds_otrainingmodel.epoch ' +
                              'FROM ds_otrainingmodel inner join ds_itrainingmodel on ds_otrainingmodel.trainingmodel_task_id = ds_itrainingmodel.trainingmodel_task_id ' +
                              'where ds_otrainingmodel.id>'+str(last_id_elasticsearch))
        self.res = cursor.fetchall()

    def insert_new_records_elasticsearch(self, msg=False):
        rows_inserted = 0
        if self.res:
            self.last_id_monetdb = self.res[-1][0]

            actions = []
            header = ['id', 'optimizer_name', 'adaptation_task_id', 'timestamp', 'elapsed_time', 'loss', 'accuracy', 'val_loss', 'val_accuracy', 'epoch']

            headerType = [
                lambda id: int(id),
                lambda optimizer_name: optimizer_name,
                lambda adaptation_task_id: adaptation_task_id,
                lambda timestamp: datetime.fromtimestamp(float(timestamp)),
                lambda elapsed_time: float(elapsed_time),
                lambda loss: loss,
                lambda accuracy: accuracy,
                lambda val_loss: val_loss,
                lambda val_accuracy: val_accuracy,
                lambda epoch: int(epoch)
            ]

            for row in self.res:
                dictCurrentRow = {column : headerType[counter](row[counter]) for counter, column in enumerate(header)}
                currentRowID = dictCurrentRow['id']
                record_for_elasticsearch = {"_id" : currentRowID, "doc": dictCurrentRow}
                actions.append(record_for_elasticsearch)

            response = helpers.bulk(self.es, actions, index=self.index_name)
            rows_inserted = self.last_id_monetdb - self.last_id_elasticsearch
            self.last_id_elasticsearch = self.last_id_monetdb
            if msg: print("Records inserted successfully!")
        else:
            if msg: print("There's nothing to be added!")

        now = datetime.now()
        current_time = now.strftime("%d-%m-%Y %H:%M:%S")
        print("Row(s) inserted: %s. Last ID: %s. Last update: %s.\n" % (rows_inserted, self.last_id_elasticsearch, current_time))