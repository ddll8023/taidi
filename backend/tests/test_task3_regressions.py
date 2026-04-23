import json
import sys
import unittest
from datetime import datetime
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.schemas.task3 import ExecutionPlan, StepType, TaskStep
from app.services.task3_executor import _validate_sql
from app.services.task3_runner import _to_jsonable


class Task3RegressionTests(unittest.TestCase):
    def test_validate_sql_rejects_hallucinated_income_sheet_columns(self):
        sql = (
            "SELECT company_code, company_name, SUM(net_profit) AS total_net_profit "
            "FROM income_sheet "
            "WHERE year = 2024 "
            "GROUP BY company_code, company_name "
            "ORDER BY total_net_profit DESC "
            "LIMIT 10"
        )

        is_valid, message = _validate_sql(sql)

        self.assertFalse(is_valid)
        self.assertIn("company_code", message)

    def test_validate_sql_accepts_task3_top10_query_with_real_columns(self):
        sql = (
            "SELECT i.stock_code, i.stock_abbr, c.company_name, i.net_profit, "
            "i.net_profit_yoy_growth, i.total_operating_revenue, "
            "i.operating_revenue_yoy_growth "
            "FROM income_sheet i "
            "LEFT JOIN company_basic_info c ON c.stock_code = i.stock_code "
            "WHERE i.report_year = 2024 AND i.report_period = 'FY' "
            "ORDER BY i.net_profit DESC "
            "LIMIT 10"
        )

        is_valid, message = _validate_sql(sql)

        self.assertTrue(is_valid, message)

    def test_to_jsonable_converts_execution_plan_datetimes(self):
        plan = ExecutionPlan(
            question="2024年利润最高的top10企业是哪些？",
            steps=[
                TaskStep(
                    step_id="s1",
                    step_type=StepType.SQL_QUERY,
                    goal="查询2024年净利润最高的top10企业",
                )
            ],
            created_at=datetime(2026, 4, 23, 13, 59, 23),
        )

        payload = _to_jsonable(plan)

        json.dumps(payload)
        self.assertEqual(payload["created_at"], "2026-04-23T13:59:23")


if __name__ == "__main__":
    unittest.main()
