import streamlit as st
import pandas as pd
import yaml

def load_rules(path="sample_rules.yaml"):
    with open(path, "r") as file:
        return yaml.safe_load(file)

def apply_rules(df, rules):
    results = []
    local_env = {"df": df, "pd": pd}
    for rule in rules:
        try:
            temp_df = df.copy()
            if "condition" in rule:
                mask = eval(rule["condition"], {}, local_env)
                temp_df = temp_df[mask]
            check_mask = eval(rule["check"], {}, {"df": temp_df, "pd": pd})
            failed = temp_df[~check_mask]
            results.append({
                "rule_id": rule["rule_id"],
                "description": rule["description"],
                "violations": len(failed),
                "percentage": round(len(failed) / len(df) * 100, 2)
            })
        except Exception as e:
            results.append({
                "rule_id": rule.get("rule_id", "unknown"),
                "description": f"Error: {str(e)}",
                "violations": -1,
                "percentage": 0
            })
    return pd.DataFrame(results)

st.title("🧹 Data Quality Check System")
df = pd.read_csv("sample_data.csv")
rules = load_rules()
result = apply_rules(df, rules)
st.dataframe(result)
