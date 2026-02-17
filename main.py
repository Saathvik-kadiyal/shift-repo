"""
FastAPI application entry point.

This module initializes the FastAPI app, configures CORS middleware,
creates database tables, and registers all API routes.
"""

from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI
from db import Base, engine
from app import route

app = FastAPI()
Base.metadata.create_all(bind=engine)

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=[
#         "*",
#     ],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # allow all origins
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)



app.include_router(route.router)

@app.get('/')
def greet():
    return 'Welcome!'

