from models.model_helper import ModelHelper


class BigQueryCheckPointModel(ModelHelper):
    """bigquery upload data"""
    collection_name = 'bigquery_check_point'

    def has_check_point(self, task_name: str, date_time: str):
        point = self.find_one({'task_name': task_name, 'date_time': date_time})
        return bool(point)

    def set_check_point(self, task_name: str, date_time: str):
        data = {'task_name': task_name, 'date_time': date_time}
        point = self.update_one(data, data, upsert=True)
        return bool(point)
