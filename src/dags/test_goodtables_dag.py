import unittest
import os
from goodtables_dag import validate_data

class TestGoodTables(unittest.TestCase):

    def test_validate(self):
      schema_path = os.getcwd() + '/schema.json'
      file_source_path = os.getcwd() + '/Water_Point_Data_Exchange_Complete_Dataset.csv'
      self.assertEqual( validate_data(file_source_path, schema_path)['valid'], True)


if __name__ == '__main__':
    unittest.main()