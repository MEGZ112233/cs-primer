import unittest
from typing import Callable

from excecuter import MemoryScan, QueryBuilder, SortMergeJoin, run
from models import DataTypes, Schema, SortMergeActions

sequence1_table = [[1], [2], [2], [2], [3], [4], [5]]
sequence2_table = [[2], [2], [3], [3], [4], [5], [6]]


def comperator(left_key, right_key, left_row, right_row):
    left_value = left_row[left_key] if left_row else None
    right_value = right_row[right_key] if right_row else None
    if left_value is None:
        return SortMergeActions.GET_LEFT_NO_MATCH
    elif right_value is None:
        return SortMergeActions.GET_RIGHT_NO_MATCH
    elif left_value > right_value:
        return SortMergeActions.GET_RIGHT_NO_MATCH
    elif left_value < right_value:
        return SortMergeActions.GET_LEFT_NO_MATCH
    elif left_value == right_value:
        return SortMergeActions.GET_RIGHT_MATCH


class TestMergeSortJoin(unittest.TestCase):
    def test_simple_test_join(self):
        schema1 = Schema(
            table_name="sequence1", columns={"number": DataTypes.INT.value}
        )

        schema2 = Schema(
            table_name="sequence2", columns={"number": DataTypes.INT.value}
        )
        # we will make equijoin

        result = list(
            run(
                QueryBuilder(
                    [
                        SortMergeJoin(comperator, "sequence1.number", "sequence2.number"),
                        MemoryScan(schema=schema1, table=sequence1_table),
                        MemoryScan(schema=schema2, table=sequence2_table),
                    ],
                    [-1, 0, 0],
                )
            )
        )
        print(result)
        self.assertEqual(len(result), 10, "")


if __name__ == "__main__":
    unittest.main()
