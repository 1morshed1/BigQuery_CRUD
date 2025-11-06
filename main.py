# main.py

from fastapi import FastAPI, HTTPException
from app.database import bigquery_client
from app.schemas import Task, TaskCreate, TaskUpdate
import uuid
from datetime import datetime, timezone
from google.cloud import bigquery
from google.api_core import exceptions as google_exceptions
from concurrent.futures import ThreadPoolExecutor
import asyncio

app = FastAPI(title="Todo API", version="1.0.0")

# Thread pool executor for running synchronous BigQuery operations
executor = ThreadPoolExecutor(max_workers=10)

def row_to_task(row) -> Task:
    """Helper function to convert BigQuery row to Task model."""
    return Task(
        id=row.id,
        title=row.title,
        description=row.description,
        status=row.status,
        created_at=row.created_at,
        updated_at=row.updated_at
    )

async def run_bigquery_query(query_func, *args, **kwargs):
    """Run synchronous BigQuery operations in thread pool executor."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, query_func, *args, **kwargs)

# CREATE - POST /tasks/
@app.post("/tasks/", response_model=Task)
async def create_task(task: TaskCreate):
    task_id = str(uuid.uuid4())
    current_time = datetime.now(timezone.utc).replace(tzinfo=None)
    
    query = f"""
    INSERT INTO `{bigquery_client.get_full_table_id()}` 
    (id, title, description, status, created_at, updated_at)
    VALUES (@id, @title, @description, @status, @created_at, @updated_at)
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("id", "STRING", task_id),
            bigquery.ScalarQueryParameter("title", "STRING", task.title),
            bigquery.ScalarQueryParameter("description", "STRING", task.description),
            bigquery.ScalarQueryParameter("status", "STRING", task.status.value),
            bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", current_time),
            bigquery.ScalarQueryParameter("updated_at", "TIMESTAMP", current_time),
        ]
    )
    
    def execute_query():
        query_job = bigquery_client.client.query(query, job_config=job_config)
        return query_job.result()
    
    try:
        await run_bigquery_query(execute_query)
        
        return Task(
            id=task_id,
            title=task.title,
            description=task.description,
            status=task.status,
            created_at=current_time,
            updated_at=current_time
        )
    except google_exceptions.BadRequest as e:
        raise HTTPException(status_code=400, detail="Invalid request parameters")
    except google_exceptions.GoogleAPICallError as e:
        raise HTTPException(status_code=500, detail="Database operation failed")
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to create task")

# READ (All) - GET /tasks/
@app.get("/tasks/", response_model=list[Task])
async def list_tasks():
    query = f"""
    SELECT id, title, description, status, created_at, updated_at
    FROM `{bigquery_client.get_full_table_id()}`
    ORDER BY created_at DESC
    """
    
    def execute_query():
        query_job = bigquery_client.client.query(query)
        return list(query_job.result())
    
    try:
        results = await run_bigquery_query(execute_query)
        tasks = [row_to_task(row) for row in results]
        return tasks
    except google_exceptions.BadRequest as e:
        raise HTTPException(status_code=400, detail="Invalid query parameters")
    except google_exceptions.GoogleAPICallError as e:
        raise HTTPException(status_code=500, detail="Database operation failed")
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to fetch tasks")

# READ (Single) - GET /tasks/{task_id}
@app.get("/tasks/{task_id}", response_model=Task)
async def get_task(task_id: str):
    query = f"""
    SELECT id, title, description, status, created_at, updated_at
    FROM `{bigquery_client.get_full_table_id()}`
    WHERE id = @task_id
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("task_id", "STRING", task_id),
        ]
    )
    
    def execute_query():
        query_job = bigquery_client.client.query(query, job_config=job_config)
        return list(query_job.result())
    
    try:
        results = await run_bigquery_query(execute_query)
        
        if not results:
            raise HTTPException(status_code=404, detail="Task not found")
        
        return row_to_task(results[0])
    except HTTPException:
        raise
    except google_exceptions.NotFound as e:
        raise HTTPException(status_code=404, detail="Task not found")
    except google_exceptions.BadRequest as e:
        raise HTTPException(status_code=400, detail="Invalid request parameters")
    except google_exceptions.GoogleAPICallError as e:
        raise HTTPException(status_code=500, detail="Database operation failed")
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to fetch task")

# UPDATE - PUT /tasks/{task_id}
@app.put("/tasks/{task_id}", response_model=Task)
async def update_task(task_id: str, task_update: TaskUpdate):
    # First, get the existing task to ensure it exists
    existing_task = await get_task(task_id)
    
    # Update only provided fields
    update_fields = {}
    if task_update.title is not None:
        update_fields['title'] = task_update.title
    if task_update.description is not None:
        update_fields['description'] = task_update.description
    if task_update.status is not None:
        update_fields['status'] = task_update.status.value
    
    update_fields['updated_at'] = datetime.now(timezone.utc).replace(tzinfo=None)
    
    if not update_fields:
        return existing_task
    
    # Build the UPDATE query dynamically
    set_clause = ", ".join([f"{field} = @{field}" for field in update_fields.keys()])
    query = f"""
    UPDATE `{bigquery_client.get_full_table_id()}`
    SET {set_clause}
    WHERE id = @task_id
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("task_id", "STRING", task_id),
        ] + [
            bigquery.ScalarQueryParameter(
                field, 
                "STRING" if field != 'updated_at' else "TIMESTAMP", 
                value
            )
            for field, value in update_fields.items()
        ]
    )
    
    def execute_query():
        query_job = bigquery_client.client.query(query, job_config=job_config)
        return query_job.result()
    
    try:
        await run_bigquery_query(execute_query)
        
        # Return updated task
        updated_status = task_update.status if task_update.status is not None else existing_task.status
        return Task(
            id=task_id,
            title=update_fields.get('title', existing_task.title),
            description=update_fields.get('description', existing_task.description),
            status=updated_status,
            created_at=existing_task.created_at,
            updated_at=update_fields['updated_at']
        )
    except google_exceptions.NotFound as e:
        raise HTTPException(status_code=404, detail="Task not found")
    except google_exceptions.BadRequest as e:
        raise HTTPException(status_code=400, detail="Invalid request parameters")
    except google_exceptions.GoogleAPICallError as e:
        raise HTTPException(status_code=500, detail="Database operation failed")
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to update task")

# DELETE - DELETE /tasks/{task_id}
@app.delete("/tasks/{task_id}")
async def delete_task(task_id: str):
    # First check if task exists (this will raise 404 if not found)
    await get_task(task_id)
    
    query = f"""
    DELETE FROM `{bigquery_client.get_full_table_id()}`
    WHERE id = @task_id
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("task_id", "STRING", task_id),
        ]
    )
    
    def execute_query():
        query_job = bigquery_client.client.query(query, job_config=job_config)
        return query_job.result()
    
    try:
        await run_bigquery_query(execute_query)
        return {"message": "Task deleted successfully"}
    except google_exceptions.NotFound as e:
        raise HTTPException(status_code=404, detail="Task not found")
    except google_exceptions.BadRequest as e:
        raise HTTPException(status_code=400, detail="Invalid request parameters")
    except google_exceptions.GoogleAPICallError as e:
        raise HTTPException(status_code=500, detail="Database operation failed")
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to delete task")

# Health check endpoint
@app.get("/")
async def root():
    return {"message": "Todo API with BigQuery"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)