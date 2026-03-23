import logging

from main import ChatRequest, chat, plan_chat_query


logging.basicConfig(level=logging.INFO)


def check(question: str) -> None:
    plan = plan_chat_query(question, [])
    assert plan.get("allowed") is True, f"plan rejected: {plan}"
    assert plan.get("source") == "flow_rule", f"unexpected planner source: {plan}"
    sql = plan.get("sql", "")
    assert "FROM billing_document_items bdi" in sql or "FROM sales_order_headers soh" in sql, sql
    assert "planner notes" not in sql.lower(), sql

    response = chat(ChatRequest(message=question, history=[]))
    assert response.get("allowed") is True, f"chat rejected: {response}"
    assert response.get("query_type") == "flow_trace", response
    assert response.get("graph_focus"), response
    print(f"[OK] {question}")
    print(response.get("answer"))
    print()


if __name__ == "__main__":
    check("Trace the full flow of billing document 90504204")
    check("Trace the full flow of sales order 740509")
