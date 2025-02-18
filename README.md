# MongoDB Chatbot Application

## Overview
The **MongoDB Chatbot** is an AI-powered chatbot that enables users to interact with MongoDB databases using natural language. It processes user queries, retrieves relevant data, and manages conversation history using an LLM (Ollama). The chatbot understands both simple and complex database queries, providing a seamless user experience.

---

## Features
✅ **Dynamic MongoDB Query Execution** - Users can query MongoDB collections without knowing exact schema details.
✅ **Supports Value-Based and Field-Specific Searches** - The chatbot intelligently searches across fields based on user queries.
✅ **Handles Aggregation Queries with Relationships** - Supports `$lookup` operations between collections.
✅ **Session-Based Database Context Management** - Remembers database and collection context across user sessions.
✅ **Conversation Memory and Summarization** - Uses LangChain’s memory module to maintain conversation history.
✅ **Optimized Query Execution and Response Processing** - Efficient query parsing and formatting for improved performance.


## Installation
### **Prerequisites**
Ensure you have the following installed:
- Python 3.8+
- MongoDB Atlas (or a local MongoDB instance)
- Ollama (LLM model)
- Required Python packages

### **Clone the Repository**
```sh
$ git clone https://github.com/your-repo/mongodb-chatbot.git
$ cd mongodb-chatbot
```

### **Install Dependencies**
```sh
$ pip install -r requirements.txt
```
