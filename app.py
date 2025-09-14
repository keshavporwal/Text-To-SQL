import streamlit as st
import requests
import pandas as pd

def generate_sql(query, tables):
    try:
        response = requests.get(
            "http://0.0.0.0:8000/generate_sql",
            json={"query": query, "tables": tables},
            headers={"Content-Type": "application/json"},
            stream=True,
            timeout=300
        )
        response.raise_for_status()
        return response.iter_content(chunk_size=None, decode_unicode=True)
    except requests.exceptions.RequestException as e:
        if hasattr(e, 'response') and e.response is not None:
            error_details = e.response.json()
            return {"error": f"Backend Error: {error_details.get('detail', str(e))}"}
        return {"error": f"Connection Error: {str(e)}"}
    
def execute_sql(sql_query):
    try:
        response = requests.get(
            "http://0.0.0.0:8000/execute_sql",
            json={"sql_query": sql_query},
            headers={"Content-Type": "application/json"},
            timeout=60
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}

def get_database_schema():
    try:
        response = requests.get(
            "http://0.0.0.0:8000/get_database_schema",
            json={},
            headers={"Content-Type": "application/json"},
            timeout=300
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        if hasattr(e, 'response') and e.response is not None:
            error_details = e.response.json()
            return {"error": f"Backend Error: {error_details.get('detail', str(e))}"}
        return {"error": f"Connection Error: {str(e)}"}
    
def main():
    st.set_page_config(page_title="Text-2-SQL", layout="wide", menu_items={})
    st.title("Text-2-SQL")

    st.markdown("""
    <style>
        .reportview-container {
            margin-top: -2em;
        }
        #MainMenu {visibility: hidden;}
        .stDeployButton {display:none;}
        footer {visibility: hidden;}
        #stDecoration {display:none;}
        .stAppToolbar {display:none;}
                
        .block-container
        {
            padding-top: 1rem;
            padding-bottom: 1rem;
            margin-top: 1rem;
        }
    </style>
""", unsafe_allow_html=True)

    filter_table_checkboxes = []
    with st.sidebar:
        st.header("Database Schema", divider="gray")
        st.subheader("Check all tables relevant to your query.")
        response = get_database_schema()
        for t in response["schema"].split("\n\n"):
            table_name = t[t.find(" ") + 1:t.find("\n")]
            t = t.replace("\n", "  \n")
            t = t.replace("Table", "**Table**")
            t = t.replace("Columns", "**Columns**")
            t = t.replace("Primary Key", "**Primary Key**")
            t = t.replace("Foreign Key", "**Foreign Key**")
            check = st.checkbox(t)
            filter_table_checkboxes.append((check, table_name))

    try:
        question = st.text_area("Enter your question in natural language:", height=100)
        
        if st.button("Generate SQL"):
            if not question:
                st.warning("Please enter a question")
                return
            
            try:
                filtered_tables = []
                for table, name in filter_table_checkboxes:
                    if table:
                        filtered_tables.append(name)
                if not filtered_tables:
                    st.warning("Please select relevant tables")
                    return
                
                response_stream = generate_sql(question, filtered_tables)
                if isinstance(response_stream, dict) and response_stream.get("error"):
                    st.error(response_stream["error"])
                else:
                    sql_output = st.empty()
                    full_response = ""
                    for token in response_stream:
                        full_response += token
                        sql_output.info(f"{full_response}")

                    if "```sql" in full_response:
                        sql_query = full_response.split("```sql")[-1].split("```")[0].strip()
                        execution_result = execute_sql(sql_query)
                        if execution_result.get("columns") and execution_result.get("data"):
                            df = pd.DataFrame(execution_result["data"], columns=execution_result["columns"])
                            st.dataframe(df, hide_index=True)
                    
            except Exception as e:
                st.error(f"Error generating SQL: {str(e)}")
        
    except Exception as e:
        st.error(f"Application error: {str(e)}")

if __name__ == "__main__":
    main()