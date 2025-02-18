from pymongo import MongoClient
from langchain_ollama import ChatOllama
from langchain.agents import initialize_agent, Tool, AgentType
import pandas as pd
import ast
from bson.decimal128 import Decimal128
from typing import List, Dict, Any
from langchain.memory import ConversationSummaryMemory
from langchain.memory.chat_message_histories import ChatMessageHistory
import chainlit as cl

class CustomMemoryManager:
    def __init__(self, llm):
        self.max_history = 5
        self.llm = llm
        self.chat_history = ChatMessageHistory()
        self.memory = ConversationSummaryMemory(
            llm=self.llm,
            chat_memory=self.chat_history,
            return_messages=True
        )
        self.displayed_history: List[Dict[str, Any]] = []
        self.interaction_count = 0
        self.previous_summary = ""

    def add_interaction(self, user_query: str, bot_response: Any) -> None:
        self.chat_history.add_user_message(user_query)
        bot_response_str = str(bot_response) if isinstance(bot_response, pd.DataFrame) else bot_response
        self.chat_history.add_ai_message(bot_response_str)
        
        self.displayed_history.append({
            "user": user_query,
            "bot": bot_response,
            "is_summary": False
        })
        
        self.interaction_count += 1
        
        if self.interaction_count % self.max_history == 0:
            self._create_and_store_summary()

    def _create_and_store_summary(self) -> None:
        new_summary = self.memory.predict_new_summary(
            messages=self.chat_history.messages,
            existing_summary=self.previous_summary
        )
        
        if self.previous_summary:
            complete_summary = f"{self.previous_summary} {new_summary}"
        else:
            complete_summary = new_summary

        self.displayed_history.append({
            "user": "Conversation Summary",
            "bot": complete_summary,
            "is_summary": True,
            "summary_at": self.interaction_count
        })
        
        self.previous_summary = complete_summary
        self.chat_history.clear()
        self.chat_history.add_user_message(f"Previous conversation context: {complete_summary}")

    def get_history(self) -> List[Dict[str, Any]]:
        return self.displayed_history

# MongoDB Connection setup
MONGODB_URI = "mongodb+srv://honikasankar:honi@cluster0.p2s1i.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(MONGODB_URI)

# Initialize LLM
ollama = ChatOllama(model="hermes3", base_url="https://ee84-2409-40f4-113-d90-44e2-2ed7-fe41-b693.ngrok-free.app/")

def execute_mongodb_query(query, page=0, page_size=60):
    try:
        if isinstance(query, str):
            query = ast.literal_eval(query)
        
        db = client[query['database']]
        collections = db.list_collection_names()
        
        if not collections:
            return f"No collections found in the database {query['database']}."
        
        print(f"Found {len(collections)} collections in database {query['database']}")
        
        # Initialize results to store data from all collections
        all_results = []
        
        # Track iteration progress
        processed_collections = 0
        matched_collections = 0

        # Execute query across all collections in the database
        for collection_name in collections:
            processed_collections += 1
            print(f"Processing collection {processed_collections}/{len(collections)}: {collection_name}")
            
            collection = db[collection_name]
            try:
                # Get the filter query
                filter_query = query.get('filter', {})
                sort_query = query.get('sort', {})
                projection = query.get('projection', None)
                
                # Create a more comprehensive search query
                search_query = {}
                
                # If filter_query contains specific values, search for them across all fields
                if isinstance(filter_query, dict) and filter_query:
                    # Get all field names in the collection
                    sample_doc = collection.find_one()
                    if sample_doc:
                        fields = list(sample_doc.keys())
                        print(f"Fields in collection {collection_name}: {fields}")
                        
                        # For each value in the filter query, search across all fields
                        or_conditions = []
                        for key, value in filter_query.items():
                            for field in fields:
                                if isinstance(value, str):
                                    or_conditions.append({
                                        field: {"$regex": value, "$options": "i"}
                                    })
                                elif isinstance(value, list):
                                    or_conditions.append({
                                        field: {"$in": value}
                                    })
                                elif isinstance(value, (int, float)):
                                    or_conditions.append({
                                        field: value
                                    })
                        
                        if or_conditions:
                            search_query = {"$or": or_conditions}
                
                print(f"Executing search query in {collection_name}: {search_query}")
                
                # Query the collection
                cursor = collection.find(search_query, projection)
                
                if sort_query:
                    cursor = cursor.sort(sort_query)
                
                collection_results = list(cursor.skip(page * page_size).limit(page_size))
                
                # Process the results
                if collection_results:
                    matched_collections += 1
                    print(f"Found {len(collection_results)} matches in {collection_name}")
                    all_results.extend(collection_results)
                else:
                    print(f"No matches found in {collection_name}")

            except Exception as e:
                print(f"Error querying {collection_name}: {str(e)}")
                continue
        
        # Print summary
        print(f"\nQuery Summary:")
        print(f"Total collections processed: {processed_collections}")
        print(f"Collections with matches: {matched_collections}")
        print(f"Total results found: {len(all_results)}")
        
        return process_results(all_results) if all_results else "No matching data found across collections."

    except Exception as e:
        return f"Error executing query: {str(e)}"
def handle_aggregation(db, collections, relationships, query):
    results = []
    pipeline = query['aggregation']
    
    for collection_name in collections:
        if collection_name in relationships:
            related_info = relationships[collection_name]
            lookup_stage = {
                '$lookup': {
                    'from': related_info['related_to'],
                    'localField': related_info['via'],
                    'foreignField': '_id',
                    'as': f'{related_info["related_to"]}_data'
                }
            }
            pipeline.insert(0, lookup_stage)
        
        try:
            collection_results = list(db[collection_name].aggregate(pipeline))
            results.extend(collection_results)
        except Exception as e:
            print(f"Error aggregating {collection_name}: {str(e)}")
            continue
    
    return results

def handle_regular_query(db, collections, relationships, query, page, page_size):
    results = []
    filter_query = query.get('filter', {})
    sort_query = query.get('sort', {})
    projection = query.get('projection', None)
    
    skip = page * page_size
    limit = page_size
    
    for collection_name in collections:
        collection = db[collection_name]
        try:
            # Modify filter_query to search by values, not just field names.
            if isinstance(filter_query, dict):
                # Loop over filter query and make sure values are included in the search
                for key, value in filter_query.items():
                    if isinstance(value, str):
                        filter_query[key] = {"$regex": value, "$options": "i"}  # Case insensitive regex search for values
                    elif isinstance(value, list):
                        filter_query[key] = {"$in": value}  # Handle list-based filtering
            
            cursor = collection.find(filter_query, projection)
            
            if sort_query:
                cursor = cursor.sort(sort_query)
            
            collection_results = list(cursor.skip(skip).limit(limit))
            
            if collection_name in relationships:
                related_info = relationships[collection_name]
                for doc in collection_results:
                    related_id = doc.get(related_info['via'])
                    if related_id:
                        related_doc = db[related_info['related_to']].find_one({'_id': related_id})
                        if related_doc:
                            doc[f'{related_info["related_to"]}_data'] = related_doc
            
            results.extend(collection_results)
        except Exception as e:
            print(f"Error querying {collection_name}: {str(e)}")
            continue
    
    return results

def process_results(results):
    if not results:
        return "No data found."
    
    processed_results = []
    for result in results:
        processed_doc = {}
        for key, value in result.items():
            if key == '_id':
                processed_doc[key] = str(value)
            elif isinstance(value, Decimal128):
                processed_doc[key] = float(value.to_decimal())
            elif isinstance(value, (dict, list)):
                processed_doc[key] = str(value)
            else:
                processed_doc[key] = value
        processed_results.append(processed_doc)
    
    return processed_results

def flatten_result(result):
    flattened_result = []
    for item in result:
        if isinstance(item, dict):
            flattened_item = {
                k: (float(v.to_decimal()) if isinstance(v, Decimal128) else str(v) if isinstance(v, (list, dict)) else v)
                for k, v in item.items()
            }
            flattened_result.append(flattened_item)
    return flattened_result


async def handle_user_query_with_tool(user_query, memory_manager):
    db_name = cl.user_session.get("db_name")
    
    # Check if database is mentioned in the current query
    if 'database' in user_query.lower():
        try:
            # Extract and store new database name
            db_parts = user_query.lower().split('database')
            new_db_name = db_parts[1].split()[0].strip('.: ')
            cl.user_session.set("db_name", new_db_name)
            db_name = new_db_name
        except IndexError:
            pass
    
    # If no database is mentioned in query but we have one stored, use it
    if db_name:
        # Only add database context if it's not already in the query
        if 'database' not in user_query.lower():
            user_query = f"Using database {db_name}, {user_query}"
    else:
        cl.user_session.set("pending_query", user_query)
        return "Please specify the database name for this query."

    try:
        response = await cl.make_async(agent.run)(user_query)
        if isinstance(response, list):
            df = pd.DataFrame(flatten_result(response))
            return df
        return response
    except Exception as e:
        return f"Error handling query: {str(e)}"
# Initialize MongoDB Tool
mongodb_tool = Tool(
    name="MongoDB Query Tool",
    func=execute_mongodb_query,
    description="Use this tool to execute MongoDB queries across all collections in a database. Input should be a dictionary with keys: database (required), filter, sort, projection, or aggregation."
)

# Initialize Agent
agent = initialize_agent(
    tools=[mongodb_tool],
    llm=ollama,
    agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
    verbose=True,
    handle_parsing_errors=True,
)

# Chainlit handlers
@cl.on_chat_start
async def start():
    cl.user_session.set("memory_manager", CustomMemoryManager(ollama))
    cl.user_session.set("db_name", None)
    cl.user_session.set("pending_query", None)
    
    await cl.Message(
        content="Welcome to MongoDB Query Chatbot! Please specify the database name in your queries."
    ).send()

@cl.on_message
async def main(message: cl.Message):
    memory_manager = cl.user_session.get("memory_manager")
    
    # Extract database name if this is a response to a pending query
    pending_query = cl.user_session.get("pending_query")
    if pending_query:
        db_name = message.content.strip().split()[0]  # Get first word as database name
        cl.user_session.set("db_name", db_name)
        combined_query = f"Using database {db_name}, {pending_query}"
        cl.user_session.set("pending_query", None)
        response = await handle_user_query_with_tool(combined_query, memory_manager)
    else:
        response = await handle_user_query_with_tool(message.content, memory_manager)
    
    # Handle response
    if isinstance(response, pd.DataFrame):
        table_message = cl.Message(content="Query Result:")
        elements = [cl.Pandas(value=response, name="Query Output")]
        await table_message.send(elements=elements)
    else:
        await cl.Message(content=str(response)).send()
    
    # Store interaction in memory
    memory_manager.add_interaction(message.content, response)
    
    # Display summary if created
    history = memory_manager.get_history()
    latest_entry = history[-1] if history else None
    if latest_entry and latest_entry.get("is_summary", False):
        await cl.Message(content=latest_entry['bot']).send()

if __name__ == "__main__":
    print("Starting MongoDB Query Chatbot...")