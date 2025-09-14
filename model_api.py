from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from transformers import AutoModelForCausalLM, AutoTokenizer, TextIteratorStreamer, StoppingCriteria
from threading import Thread, Event
from sql import get_database_schema, get_normalized_create_statement, execute_sql_query

model_name = "Qwen/Qwen2.5-Coder-7B-Instruct-GPTQ-Int4"
model = AutoModelForCausalLM.from_pretrained(
    model_name,
    torch_dtype="auto",
    device_map="auto"
)
tokenizer = AutoTokenizer.from_pretrained(model_name)

model.eval()

app = FastAPI(
    title="Text-2-SQL API",
    description="API for converting natural language text to SQL and executing queries",
    version="1.0"
)

class QueryRequest(BaseModel):
    query: str
    tables: list[str]

class ExecuteRequest(BaseModel):
    sql_query: str

class SchemaRequest(BaseModel):
    None


def create_prompt(question, filtered_tables):
    schema = get_normalized_create_statement(filtered_tables)
    return f"""You are a data science expert. Below, you are provided with a database schema and a natural language question. Your task is to understand the schema and generate a valid PostgreSQL query to answer the question.
    
Database Schema:
{schema}

Question:
{question}

Instructions:
- Make sure you only output the information that is asked in the question. If the question asks for a specific column, make sure to only include that column in the SELECT clause, nothing more.
- The generated query should return all of the information asked in the question without any missing or extra information.
- Before generating the final SQL query, please think through the steps of how to write the query. Do all the explanation before generating the final query.
- Make sure to check the datatypes of the columns. For Example: if Date column has text datatype, do not use date functions on it, use string functions.
- If you think some table information is missing or the database schema provided has no relevance with the question, do not answer with any SQL query.

Take a deep breath and think step by step to find the correct SQL query.
"""


generation_controller = {
    "thread": None,
    "stop_event": Event()
}

@app.get("/generate_sql")
async def get_sql(request: QueryRequest):
    """Endpoint for generating SQL from natural language"""
    try:
        if generation_controller["thread"] and generation_controller["thread"].is_alive():
            generation_controller["stop_event"].set()
            generation_controller["thread"].join(timeout=1)

        generation_controller["stop_event"] = Event()

        question = request.query
        tables = request.tables

        prompt = create_prompt(question, tables)
        messages = [
            {"role": "system", "content": "You are Qwen, created by Alibaba Cloud. You are a helpful assistant."},
            {"role": "user", "content": prompt}
        ]

        text = tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True
        )
        model_inputs = tokenizer([text], return_tensors="pt").to(model.device)


        streamer = TextIteratorStreamer(tokenizer, skip_prompt=True, skip_special_tokens=True)

        class StopOnEvent(StoppingCriteria):
            def __init__(self, stop_event):
                self.stop_event = stop_event

            def __call__(self, *args, **kwargs):
                return self.stop_event.is_set()

        thread = Thread(
            target=model.generate,
            kwargs={
                **model_inputs,
                "max_new_tokens": 1024,
                "streamer": streamer,
                "stopping_criteria": [StopOnEvent(generation_controller["stop_event"])]
            }
        )
        thread.start()
        generation_controller["thread"] = thread
        
        def stream_tokens(streamer):
            for token in streamer:
                if generation_controller["stop_event"].is_set():
                    break
                yield token

        return StreamingResponse(stream_tokens(streamer), media_type="text/plain")
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Unexpected error: {str(e)}\n")
        raise HTTPException(status_code=500, detail="Internal server error")
    
@app.get("/execute_sql")
def execute_sql(request: ExecuteRequest):
    try:
        result = execute_sql_query(request.sql_query)
        return result
    except Exception as e:
        return {"error": str(e)}

@app.get("/get_database_schema")
async def get_schema(request: SchemaRequest):
    return {
        "status": "success",
        "schema": get_database_schema()
    }