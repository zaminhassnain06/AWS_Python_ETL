class TableItem:

    def __init__(self, json_object, row):
        if json_object is not None:
            self.table_name = json_object['table_name']
            self.batch_size = json_object['batch_size']
            self.column_name = json_object['column_name']
            self.starting_value = json_object['starting_value']
            self.column_type = json_object['column_type']
            self.batch_id = ''
        else:
            self.table_name = row['source_table']
            self.batch_size = row['data_size']
            self.column_name = row['source_column']
            self.starting_value = row['column_end']
            self.column_type = row['source_type']
            self.batch_id = row['batch_id']
