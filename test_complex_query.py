import json
import logging
from main import plan_chat_query, execute_select_sql, summarize_rows, MAX_CHAT_ROWS

# Configure logging to see what's happening
logging.basicConfig(level=logging.INFO)

def test_complex_query(question):
    print(f"\nUser Question: {question}")
    print("-" * 50)
    
    # 1. Plan the query using the Groq planner
    plan = plan_chat_query(question, [])
    
    if not plan.get("allowed"):
        print(f"Plan Rejected: {plan.get('reason')}")
        if "llm_error" in plan:
            print(f"Error: {plan['llm_error']}")
        return

    print(f"Planner Reason: {plan['reason']}")
    print(f"Generated SQL:\n{plan['sql']}")
    print("-" * 50)

    # 2. Execute the SQL
    try:
        columns, rows, truncated = execute_select_sql(plan["sql"], MAX_CHAT_ROWS)
        
        # 3. Summarize the results
        answer = summarize_rows(columns, rows, truncated)
        print(f"Assistant Answer: {answer}")
        
        if rows:
            print("\nSample Results (First 3):")
            for i, row in enumerate(rows[:3]):
                print(f"  {i+1}: {row}")
        else:
            print("No rows returned (data is clean or joins failed).")
            
    except Exception as e:
        print(f"Execution Error: {e}")

if __name__ == "__main__":
    query = "Identify sales orders that have broken or incomplete flows (e.g. delivered but not billed, billed without delivery)"
    test_complex_query(query)
