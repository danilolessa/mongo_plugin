from bson import json_util
import json
import os

from mongo_plugin.hooks.mongo_hook import MongoHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator


class MongoToGCSOperator(BaseOperator):
    """
    Mongo -> GCS
    :param mongo_conn_id:           The source mongo connection id.
    :type mongo_conn_id:            string
    :param mongo_collection:        The source mongo collection.
    :type mongo_collection:         string
    :param mongo_database:          The source mongo database.
    :type mongo_database:           string
    :param mongo_query:             The specified mongo query.
    :type mongo_query:              string
    :param gcs_bucket:               The destination gcs bucket.
    :type gcs_bucket:                string
    :param gcs_object:                  The destination gcs filepath.
    :type gcs_object:                   string
    :param gcs_conn_id:              The destination gcs connnection id.
    :type gcs_conn_id:               string
    """

    # TODO This currently sets job = queued and locks job
    template_fields = ['gcs_object', 'mongo_query']

    def __init__(self,
                 mongo_conn_id,
                 mongo_collection,
                 mongo_database,
                 mongo_query,
                 gcs_bucket,
                 gcs_object,
                 gcs_conn_id,
                 *args, **kwargs):
        super(MongoToGCSOperator, self).__init__(*args, **kwargs)
        # Conn Ids
        self.mongo_conn_id = mongo_conn_id
        self.gcs_conn_id = gcs_conn_id
        # Mongo Query Settings
        self.mongo_db = mongo_database
        self.mongo_collection = mongo_collection
        # Grab query and determine if we need to run an aggregate pipeline
        self.mongo_query = mongo_query
        self.is_pipeline = True if isinstance(self.mongo_query, list) else False

        # GCS Settings
        self.gcs_bucket = gcs_bucket
        self.gcs_object = gcs_object

        # KWARGS
        self.replace = kwargs.pop('replace', False)

    def execute(self, context):
        """
        Executed by task_instance at runtime
        """
        mongo_conn = MongoHook(self.mongo_conn_id).get_conn()
        gcs_conn = GoogleCloudStorageHook(self.gcs_conn_id)

        # Grab collection and execute query according to whether or not it is a pipeline
        collection = mongo_conn.get_database(self.mongo_db).get_collection(self.mongo_collection)
        results = collection.aggregate(self.mongo_query) if self.is_pipeline else collection.find(self.mongo_query)

        # Performs transform then stringifies the docs results into json format
        docs_str = self._stringify(self.transform(results))
        with open("__temp__", "w") as fid:
            fid.write(docs_str)

        gcs_conn.upload(self.gcs_bucket, self.gcs_object, "__temp__")

        #os.remove("__temp__")
        #s3_conn.load_string(docs_str, self.s3_key, bucket_name=self.s3_bucket, replace=self.replace)

    def _stringify(self, iter, joinable='\n'):
        """
        Takes an interable (pymongo Cursor or Array) containing dictionaries and
        returns a stringified version using python join
        """
        return joinable.join([json.dumps(doc, default=json_util.default) for doc in iter])

    def transform(self, docs):
        """
        Processes pyMongo cursor and returns single array with each element being
                a JSON serializable dictionary
        MongoToGCSOperator.transform() assumes no processing is needed
        ie. docs is a pyMongo cursor of documents and cursor just needs to be
            converted into an array.
        """
        return [doc for doc in docs]
