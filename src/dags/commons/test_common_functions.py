import unittest
from common_functions import validate_mandatory, check_difference_between_two_lists, validate_config, traverse_json

class TestCustomFunctions(unittest.TestCase):

    def test_check_difference_between_two_lists(self):
      subset = [1,2]
      superset = [1,2,3,4,5]
      self.assertEqual(check_difference_between_two_lists(subset, superset,"Test"), True)


if __name__ == '__main__':
    unittest.main()
