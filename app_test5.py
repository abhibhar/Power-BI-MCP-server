import os
import asyncio
from dotenv import load_dotenv
from contextlib import asynccontextmanager

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import uvicorn

from langchain_core.messages import HumanMessage
from langgraph.checkpoint.memory import InMemorySaver
try:
    from langchain.agents import create_react_agent # type: ignore
except ImportError:
    from langgraph.prebuilt import create_react_agent
from langchain_openai import ChatOpenAI
from langchain_mcp_adapters.tools import load_mcp_tools

from difflib import SequenceMatcher
from typing import List, Dict, Optional

# --------------------------------------------------
# Fuzzy Matching Functions
# --------------------------------------------------
def fuzzy_match(query: str, options: List[str], threshold: float = 0.6) -> List[Dict[str, any]]:
    """
    Fuzzy match a query against a list of options
    Returns list of matches with scores, sorted by best match
    """
    matches = []
    query_lower = query.lower()
    
    for option in options:
        option_lower = option.lower()
        
        # Exact match (case-insensitive)
        if query_lower == option_lower:
            matches.append({'name': option, 'score': 1.0, 'match_type': 'exact'})
            continue
        
        # Contains match
        if query_lower in option_lower or option_lower in query_lower:
            score = 0.9
            matches.append({'name': option, 'score': score, 'match_type': 'contains'})
            continue
        
        # Similarity ratio
        ratio = SequenceMatcher(None, query_lower, option_lower).ratio()
        if ratio >= threshold:
            matches.append({'name': option, 'score': ratio, 'match_type': 'fuzzy'})
    
    # Sort by score (highest first)
    matches.sort(key=lambda x: x['score'], reverse=True)
    return matches


async def search_tables_and_columns(mcp_session, table_query: Optional[str] = None, 
                                   column_query: Optional[str] = None) -> Dict:
    """
    Search for tables and columns in the Power BI model using MCP tools
    Returns fuzzy-matched results
    """
    result = {
        'tables': [],
        'columns': [],
        'table_matches': [],
        'column_matches': []
    }
    
    try:
        # Try to list all tables using MCP tools
        tools = await load_mcp_tools(mcp_session)
        
        # Look for a tool that can list tables/schema
        list_tool = None
        for tool in tools:
            tool_name_lower = tool.name.lower()
            if any(keyword in tool_name_lower for keyword in ['list', 'get', 'schema', 'tables', 'describe']):
                list_tool = tool
                break
        
        if list_tool:
            # Try to get schema/table list
            try:
                schema_result = await list_tool.ainvoke({})
                # Parse the result to extract table and column names
                
                if isinstance(schema_result, dict):
                    if 'tables' in schema_result:
                        result['tables'] = schema_result['tables']
                    if 'columns' in schema_result:
                        result['columns'] = schema_result['columns']
                        
            except Exception as e:
                print(f"[DEBUG] Could not fetch schema: {e}")
        
        # Perform fuzzy matching if queries provided
        if table_query and result['tables']:
            result['table_matches'] = fuzzy_match(table_query, result['tables'])
        
        if column_query and result['columns']:
            result['column_matches'] = fuzzy_match(column_query, result['columns'])
            
    except Exception as e:
        print(f"[DEBUG] Error in search_tables_and_columns: {e}")
    
    return result


def detect_user_intent(user_input: str) -> str:
    """Returns 'show', 'create', 'update', 'delete', or 'unknown'"""
    lower = user_input.lower()
    
    if any(w in lower for w in ['show', 'display', 'view', 'get', 'what is', 'tell me', 'give me']):
        return 'show'
    
    if any(w in lower for w in ['create', 'add', 'make', 'build', 'new']):
        return 'create'
    
    if any(w in lower for w in ['update', 'modify', 'change', 'edit']):
        return 'update'
    
    if any(w in lower for w in ['delete', 'remove', 'drop']):
        return 'delete'
    
    return 'unknown'


def parse_measure_request(user_input: str) -> dict:
    """Extract table, column, and operation hints"""
    import re
    lower = user_input.lower()
    
    # Detect operation type
    op = None
    if any(w in lower for w in ['total', 'sum']): op = 'SUM'
    elif any(w in lower for w in ['average', 'avg', 'mean']): op = 'AVERAGE'
    elif any(w in lower for w in ['count', 'number']): op = 'COUNT'
    elif any(w in lower for w in ['unique', 'distinct']): op = 'DISTINCTCOUNT'
    elif any(w in lower for w in ['min', 'minimum']): op = 'MIN'
    elif any(w in lower for w in ['max', 'maximum']): op = 'MAX'
    
    # Extract Table[Column] if present
    match = re.search(r'([A-Za-z_]\w*)\[(\w+)\]', user_input, re.IGNORECASE)
    
    return {
        'operation': op,
        'table': match.group(1) if match else None,
        'column': match.group(2) if match else None,
        'has_explicit_syntax': match is not None
    }


# --------------------------------------------------
# Load environment variables
# --------------------------------------------------
load_dotenv()

# Global variables
agent_instance = None
mcp_session = None
agent_initialized = False
initialization_error = None

# Chat history storage (in production, use a database)
chat_sessions = {}

# --------------------------------------------------
# System Prompt
# --------------------------------------------------
AGENT_SYSTEM_PROMPT = """You are a Power BI assistant with intelligent search capabilities.

# CORE RULES

## 1. SMART SEARCH FIRST
- Search for table/column names using available MCP tools
- Handle typos and variations with fuzzy matching
- Execute when 1 clear match found (score > 0.8)
- Ask when 2-3 similar matches found

## 2. UNDERSTAND INTENT
- "show/display/what is/get" → QUERY data (use query tools)
- "create/add/make measure" → CREATE measure (use creation tools)
- Ambiguous → Default to QUERY

## 3. CASE-INSENSITIVE & FUZZY MATCHING
- "sales" matches "Sales", "SalesTable", "Sales_Data"
- "amt" matches "Amount", "SalesAmount"
- "cust" matches "Customer", "Customers"

## 4. AUTO-CONSTRUCT DAX

### Basic Aggregations:
- "total sales" → SUM(Sales[Amount])
- "average price" → AVERAGE(Products[Price])
- "count customers" → COUNTROWS(Customers) or DISTINCTCOUNT(Customers[ID])

### Time Intelligence (CRITICAL):
**For Year-over-Year (YoY) calculations:**

When user asks for YoY growth:
1. You MUST create a DAX measure, not calculate manually
2. Use SAMEPERIODLASTYEAR or DATEADD functions
3. Format: 
```
   YoY Growth % = 
   VAR CurrentYear = SUM(Table[Sales])
   VAR PreviousYear = CALCULATE(SUM(Table[Sales]), SAMEPERIODLASTYEAR(DateTable[Date]))
   RETURN
   DIVIDE(CurrentYear - PreviousYear, PreviousYear, 0)
```

**Important Rules for Time Intelligence:**
- ALWAYS ask for the date table name if not obvious
- Use CALCULATE for time-shifted calculations
- Use DIVIDE to avoid division by zero
- For YoY%, multiply by 100 or format as percentage

## 5. NEVER CALCULATE MANUALLY

❌ WRONG Approach:
User: "Show YoY growth"
You: [Query data, then manually calculate in response]

✅ CORRECT Approach:
User: "Show YoY growth"
You: [Create YoY measure with DAX, then query to show results]

**Critical Rule**: If you need calculations, create a DAX measure first. DO NOT calculate in your text response.

# DECISION LOGIC

1. **Understand request** (query vs create vs time intelligence)
2. **Check if calculation needed**:
   - Simple aggregation → Query or create measure
   - Time intelligence → MUST create measure first
   - Complex calculation → MUST create measure first
3. **Search for names** (tables/columns/date tables)
4. **Execute**: Create measure if needed, then query

# EXAMPLES

**Example 1: Simple Query**
User: "show total sales"
You: [Query with existing measure or SUM]
Result: Display total

**Example 2: YoY Growth (CORRECT)**
User: "show YoY growth from 2013 to 2014"
You: 
1. [Check if YoY measure exists]
2. [If not, CREATE measure with SAMEPERIODLASTYEAR]
3. [THEN query the data]
Result: Display accurate YoY percentages per product

**Example 3: YoY Growth (WRONG - Don't do this)**
User: "show YoY growth"
You: [Query sales by year, then calculate (2014-2013)/2013 in text]
❌ This is WRONG - creates inconsistent results

**Example 4: Product Sales by Year**
User: "show product, year, sales"
You: [Query Financials table with Product, Year, Sales columns]
Result: Display table with all rows

# COMPLEX CALCULATIONS - MUST USE DAX

For these requests, ALWAYS create a measure first:
- YoY Growth, YoY %, Year-over-Year
- Month-over-Month (MoM)
- Running Total, Cumulative Sum
- Percentage of Total
- Rank, Top N
- Moving Average
- Same Period Last Year (SPLY)

**Workflow for Complex Calculations:**
1. Ask for date table if needed for time intelligence
2. Create the DAX measure
3. Query the data using that measure
4. Display results

# ACCURACY CHECKLIST

Before responding with data:
□ Did I query the data source (not calculate manually)?
□ For time intelligence, did I create a proper DAX measure?
□ Are the numbers coming from Power BI (not my calculations)?
□ If creating measure, is the DAX syntax correct?

# RESPONSES
- Simple queries: Execute and display
- Complex calculations: Create measure FIRST, then query
- Multiple matches: Show numbered options
- Always show data from Power BI, not manual calculations

Be smart, accurate, and always use DAX for calculations."""

# --------------------------------------------------
# HTML UI with Collapsible Sidebar and Chat History
# --------------------------------------------------
HTML_CONTENT = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Power BI Assistant</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        :root {
            --sidebar-width: 250px;
            --sidebar-collapsed-width: 0px;
            --header-height: 40px;
            --primary-gold: #F2C811;
            --dark-gold: #E0B50F;
            --cream: #FFF4DC;
            --off-white: #FFF9E6;
            --light-yellow: #FFF9E6;
            --dark: #1A1A1A;
            --medium-dark: #2C2C2C;
            --text-primary: #1F1F1F;
            --text-secondary: #666666;
            --border: #E0E0E0;
            --shadow: rgba(0, 0, 0, 0.1);
        }

        body {
            font-family: 'Outfit', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: var(--cream);
            height: 100vh;
            display: flex;
            overflow: hidden;
        }

        /* Sidebar Styles */
        .sidebar {
            width: var(--sidebar-width);
            min-width: var(--sidebar-width);
            background: var(--dark);
            display: flex;
            flex-direction: column;
            border-right: 1px solid rgba(255, 255, 255, 0.1);
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            overflow: hidden;
            position: relative;
        }

        .sidebar.collapsed {
            width: 0;
            min-width: 0;
            border-right: none;
        }

        .sidebar-content {
            display: flex;
            flex-direction: column;
            height: 100%;
            min-width: var(--sidebar-width);
            opacity: 1;
            transition: opacity 0.1s ease;
        }

        .sidebar.collapsed .sidebar-content {
            opacity: 0;
            pointer-events: none;
        }

        .sidebar-header {
            padding: 16px;
            border-bottom: 1px solid rgba(255, 255, 255, 0.1);
        }

        .new-chat-btn {
            width: 100%;
            padding: 12px 16px;
            background: transparent;
            border: 1px solid rgba(255, 255, 255, 0.2);
            border-radius: 8px;
            color: white;
            font-size: 14px;
            font-weight: 500;
            cursor: pointer;
            display: flex;
            align-items: center;
            gap: 10px;
            transition: all 0.2s;
            font-family: 'Outfit', sans-serif;
        }

        .new-chat-btn:hover {
            background: rgba(255, 255, 255, 0.1);
            border-color: rgba(255, 255, 255, 0.3);
        }

        .new-chat-btn svg {
            width: 18px;
            height: 18px;
        }

        .chat-list {
            flex: 1;
            overflow-y: auto;
            padding: 8px;
        }

        .chat-section-title {
            color: rgba(255, 255, 255, 0.5);
            font-size: 11px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            padding: 12px 12px 8px 12px;
        }

        .chat-item {
            padding: 10px 12px;
            margin: 2px 0;
            border-radius: 8px;
            color: rgba(255, 255, 255, 0.8);
            font-size: 14px;
            cursor: pointer;
            transition: all 0.2s;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }

        .chat-item:hover {
            background: rgba(255, 255, 255, 0.1);
            color: white;
        }

        .chat-item.active {
            background: rgba(242, 200, 17, 0.15);
            color: var(--primary-gold);
        }

        .chat-item-text {
            flex: 1;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .chat-item-delete {
            opacity: 0;
            padding: 4px;
            border-radius: 4px;
            transition: all 0.2s;
            display: flex;
            align-items: center;
            justify-content: center;
        }

        .chat-item:hover .chat-item-delete {
            opacity: 1;
        }

        .chat-item-delete:hover {
            background: rgba(255, 0, 0, 0.2);
        }

        .chat-item-delete svg {
            width: 14px;
            height: 14px;
        }

        .sidebar-footer {
            padding: 16px;
            border-top: 1px solid rgba(255, 255, 255, 0.1);
        }

        .user-profile {
            display: flex;
            align-items: center;
            gap: 12px;
            padding: 10px;
            border-radius: 8px;
            cursor: pointer;
            transition: background 0.2s;
        }

        .user-profile:hover {
            background: rgba(255, 255, 255, 0.05);
        }

        .user-avatar {
            width: 32px;
            height: 32px;
            border-radius: 50%;
            background: linear-gradient(135deg, var(--primary-gold), #E67E22);
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 600;
            font-size: 14px;
            color: var(--dark);
        }

        .user-info {
            flex: 1;
        }

        .user-name {
            color: white;
            font-size: 14px;
            font-weight: 500;
        }

        .user-email {
            color: rgba(255, 255, 255, 0.5);
            font-size: 12px;
        }

        /* Main Content Area */
        .main-content {
            flex: 1;
            display: flex;
            flex-direction: column;
            overflow: hidden;
            transition: margin-left 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        }

        .header {
            height: var(--header-height);
            background: #F2C811;
            border-bottom: 1px solid var(--dark-gold);
            padding: 0 24px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            flex-shrink: 0;
        }

        .header-left {
            display: flex;
            align-items: center;
            gap: 16px;
        }

        .sidebar-toggle {
            width: 36px;
            height: 36px;
            background: transparent;
            border: 1px solid var(--dark-gold);
            border-radius: 6px;
            cursor: pointer;
            display: flex;
            align-items: center;
            justify-content: center;
            transition: all 0.2s;
            color: var(--text-primary);
        }

        .sidebar-toggle:hover {
            background: rgba(242, 200, 17, 0.2);
            border-color: var(--primary-gold);
        }

        .sidebar-toggle svg {
            width: 20px;
            height: 20px;
            transition: transform 0.3s ease;
        }

        .sidebar-toggle.rotated svg {
            transform: rotate(180deg);
        }

        .header h1 {
            font-size: 18px;
            font-weight: 600;
            color: var(--text-primary);
        }

        .status-indicator {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: #ffc107;
            animation: pulse 2s infinite;
        }

        .status-indicator.connected {
            background: #10a37f;
            animation: none;
        }

        .status-indicator.error {
            background: #ef4444;
            animation: none;
        }

        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }

        .chat-container {
            flex: 1;
            overflow-y: auto;
            display: flex;
            flex-direction: column;
        }

        .messages-wrapper {
            flex: 1;
            display: flex;
            flex-direction: column;
        }

        .message {
            border-bottom: 1px solid var(--border);
            padding: 32px 24px;
            animation: fadeIn 0.3s ease-out;
        }

        @keyframes fadeIn {
            from {
                opacity: 0;
                transform: translateY(10px);
            }
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }

        .message.user {
            background: var(--off-white);
        }

        .message.assistant {
            background: white;
        }

        .message-content {
            max-width: 800px;
            margin: 0 auto;
            width: 100%;
            display: flex;
            gap: 24px;
        }

        .avatar {
            width: 36px;
            height: 36px;
            border-radius: 50%;
            flex-shrink: 0;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 600;
            font-size: 15px;
        }

        .message.user .avatar {
            background: linear-gradient(135deg, var(--primary-gold), #E67E22);
            color: var(--dark);
        }

        .message.assistant .avatar {
            background: var(--medium-dark);
            color: white;
        }

        .message-text {
            flex: 1;
            color: var(--text-primary);
            line-height: 1.7;
            font-size: 15px;
            white-space: pre-wrap;
            word-wrap: break-word;
        }

        .message-text table {
            border-collapse: collapse;
            width: 100%;
            margin: 16px 0;
            font-size: 14px;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            border: 1px solid var(--border);
            box-shadow: 0 2px 8px var(--shadow);
        }

        .message-text table th,
        .message-text table td {
            border: 1px solid var(--border);
            padding: 12px 16px;
            text-align: left;
        }

        .message-text table th {
            background: var(--primary-gold);
            color: var(--dark);
            font-weight: 600;
        }

        .message-text table tr:hover {
            background: var(--off-white);
        }

        .typing-indicator {
            display: none;
        }

        .typing-indicator.active {
            display: flex;
            gap: 4px;
            padding: 8px 0;
        }

        .typing-indicator span {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: var(--primary-gold);
            animation: bounce 1.4s infinite ease-in-out;
        }

        .typing-indicator span:nth-child(1) { animation-delay: -0.32s; }
        .typing-indicator span:nth-child(2) { animation-delay: -0.16s; }

        @keyframes bounce {
            0%, 80%, 100% {
                transform: scale(0);
                opacity: 0.5;
            }
            40% {
                transform: scale(1);
                opacity: 1;
            }
        }

        .input-area {
            padding: 24px;
            background: white;
            border-top: 1px solid var(--border);
            flex-shrink: 0;
        }

        .input-container {
            max-width: 800px;
            margin: 0 auto;
            position: relative;
            display: flex;
            align-items: flex-end;
            background: white;
            border-radius: 12px;
            border: 2px solid var(--border);
            box-shadow: 0 2px 12px var(--shadow);
            transition: border-color 0.2s;
        }

        .input-container:focus-within {
            border-color: var(--primary-gold);
        }

        #messageInput {
            flex: 1;
            padding: 14px 50px 14px 18px;
            background: transparent;
            border: none;
            color: var(--text-primary);
            font-size: 15px;
            outline: none;
            resize: none;
            max-height: 200px;
            overflow-y: auto;
            line-height: 1.5;
            font-family: 'Outfit', sans-serif;
        }

        #messageInput::placeholder {
            color: var(--text-secondary);
        }

        #sendButton {
            position: absolute;
            right: 8px;
            bottom: 8px;
            width: 36px;
            height: 36px;
            background: var(--primary-gold);
            border: none;
            border-radius: 8px;
            cursor: pointer;
            display: flex;
            align-items: center;
            justify-content: center;
            transition: all 0.2s;
        }

        #sendButton:hover:not(:disabled) {
            background: var(--dark-gold);
            transform: scale(1.05);
        }

        #sendButton:disabled {
            background: var(--border);
            cursor: not-allowed;
            opacity: 0.5;
        }

        #sendButton svg {
            width: 18px;
            height: 18px;
            color: var(--dark);
        }

        .footer-text {
            text-align: center;
            color: var(--text-secondary);
            font-size: 12px;
            margin-top: 12px;
        }

        .welcome-message {
            max-width: 800px;
            margin: auto;
            padding: 64px 24px;
            color: var(--text-primary);
            text-align: center;
        }

        .welcome-message h2 {
            font-size: 42px;
            font-weight: 700;
            margin-bottom: 16px;
            background: linear-gradient(135deg, var(--primary-gold), #E67E22);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            letter-spacing: -0.5px;
        }

        .welcome-message p {
            font-size: 16px;
            color: var(--text-secondary);
            margin-bottom: 48px;
            font-weight: 300;
        }

        .capabilities {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 25px;
            margin-top: 48px;
        }

        .capability-card {
            background: white;
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 24px;
            text-align: left;
            transition: all 0.3s;
            cursor: default;
            width: 110%
        }

        .capability-card:hover {
            transform: translateY(-4px);
            box-shadow: 0 8px 24px var(--shadow);
            border-color: var(--primary-gold);
        }

        .capability-card h3 {
            font-size: 16px;
            font-weight: 600;
            margin-bottom: 8px;
            color: var(--text-primary);
        }

        .capability-card p {
            font-size: 14px;
            color: var(--text-secondary);
            line-height: 1.6;
            margin: 0;
        }
#clearChatButton {
            padding: 6px 16px;
            background: #FFFFFF;
            border: 1px solid #E0B50F;
            border-radius: 4px;
            cursor: pointer;
            font-weight: 600;
            font-size: 12px;
            color: #1F1F1F;
            font-family: 'Comic Sans MS', cursive, sans-serif;
        }
        #clearChatButton:hover {
            background: #F5F5F5;}
        /* Scrollbar Styles */
        ::-webkit-scrollbar {
            width: 8px;
        }

        ::-webkit-scrollbar-track {
            background: rgba(0, 0, 0, 0.05);
        }

        ::-webkit-scrollbar-thumb {
            background: rgba(0, 0, 0, 0.2);
            border-radius: 4px;
        }

        ::-webkit-scrollbar-thumb:hover {
            background: rgba(0, 0, 0, 0.3);
        }

        /* Mobile Responsive */
        @media (max-width: 768px) {
            .sidebar {
                position: fixed;
                left: 0;
                top: 0;
                height: 100%;
                z-index: 1000;
                transform: translateX(-100%);
            }

            .sidebar.collapsed {
                transform: translateX(-100%);
            }

            .sidebar.open {
                transform: translateX(0);
                width: var(--sidebar-width);
                min-width: var(--sidebar-width);
            }

            .main-content {
                width: 100%;
            }

            .capabilities {
                grid-template-columns: 1fr;
            }
        }

        /* Overlay for mobile */
        .sidebar-overlay {
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0, 0, 0, 0.5);
            z-index: 999;
        }

        .sidebar-overlay.active {
            display: block;
        }
    </style>
</head>
<body>
    <!-- Sidebar -->
    <div class="sidebar" id="sidebar">
        <div class="sidebar-content">
            <div class="sidebar-header">
                <button class="new-chat-btn" id="newChatBtn">
                    <svg stroke="currentColor" fill="none" stroke-width="2" viewBox="0 0 24 24" stroke-linecap="round" stroke-linejoin="round">
                        <line x1="12" y1="5" x2="12" y2="19"></line>
                        <line x1="5" y1="12" x2="19" y2="12"></line>
                    </svg>
                    New chat
                </button>
            </div>

            <div class="chat-list" id="chatList">
                <!-- Chat history will be dynamically inserted here -->
            </div>

            <div class="sidebar-footer">
                <div class="user-profile">
                    <div class="user-avatar">U</div>
                    <div class="user-info">
                        <div class="user-name">Abhishek B</div>
                        <div class="user-email">Manage account</div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <!-- Sidebar Overlay (for mobile) -->
    <div class="sidebar-overlay" id="sidebarOverlay"></div>

    <!-- Main Content -->
    <div class="main-content">
        <div class="header">
            <div class="header-left">
                <button class="sidebar-toggle" id="sidebarToggle">
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <path d="M3 12h18M3 6h18M3 18h18"/>
                    </svg>
                </button>
                <h1>Power BI Assistant</h1>
                <div id="statusIndicator" class="status-indicator"></div>
            </div>
        </div>

        <div class="chat-container" id="chatContainer">
            <div class="messages-wrapper" id="messagesWrapper">
                <div class="welcome-message">
                    <h2>What's on your mind today?</h2>
                    <p>I can help you create, update, and delete measures and calculated columns in your Power BI models.</p>
                    
                    <div class="capabilities">
                        <div class="capability-card">
                            <h3>📊 Create Measures</h3>
                            <p>Define new DAX measures for your data model with intelligent suggestions</p>
                        </div>
                        <div class="capability-card">
                            <h3>✏️ Update Measures</h3>
                            <p>Modify existing measure definitions and optimize DAX formulas</p>
                        </div>
                        <div class="capability-card">
                            <h3>🗑️ Delete Measures</h3>
                            <p>Remove measures you no longer need from your model</p>
                        </div>
                        <div class="capability-card">
                            <h3>➕ Calculated Columns</h3>
                            <p>Create new calculated columns in tables with proper context</p>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <div class="input-area">
            <div class="input-container">
                <textarea 
                    id="messageInput" 
                    placeholder="Message Power BI Assistant..." 
                    rows="1"
                    autocomplete="off"
                ></textarea>
                <button id="sendButton" disabled>
                    <svg stroke="currentColor" fill="none" stroke-width="2" viewBox="0 0 24 24" stroke-linecap="round" stroke-linejoin="round">
                        <line x1="22" y1="2" x2="11" y2="13"></line>
                        <polygon points="22 2 15 22 11 13 2 9 22 2"></polygon>
                    </svg>
                </button>
            </div>
            <div class="footer-text">ISO 2025 All rights reserved</div>
        </div>
    </div>

    <script>
        // Global Variables
        const chatContainer = document.getElementById('chatContainer');
        const messagesWrapper = document.getElementById('messagesWrapper');
        const messageInput = document.getElementById('messageInput');
        const sendButton = document.getElementById('sendButton');
        const statusIndicator = document.getElementById('statusIndicator');
        const sidebar = document.getElementById('sidebar');
        const sidebarToggle = document.getElementById('sidebarToggle');
        const sidebarOverlay = document.getElementById('sidebarOverlay');
        const newChatBtn = document.getElementById('newChatBtn');
        const chatList = document.getElementById('chatList');

        let ws = null;
        let isConnected = false;
        let reconnectAttempts = 0;
        const maxReconnectAttempts = 5;
        let hasMessages = false;
        let currentChatId = null;

        // Generate unique chat ID
        function generateChatId() {
            return 'chat_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
        }

        // Sidebar toggle functionality
        sidebarToggle.addEventListener('click', () => {
            sidebar.classList.toggle('collapsed');
            sidebarToggle.classList.toggle('rotated');
            
            // For mobile
            if (window.innerWidth <= 768) {
                sidebar.classList.toggle('open');
                sidebarOverlay.classList.toggle('active');
            }
        });

        sidebarOverlay.addEventListener('click', () => {
            sidebar.classList.remove('open');
            sidebarOverlay.classList.remove('active');
        });

        // Load chat history from server
        async function loadChatHistory() {
            try {
                const response = await fetch('/api/chats');
                const data = await response.json();
                renderChatHistory(data);
            } catch (error) {
                console.error('Failed to load chat history:', error);
            }
        }

        // Render chat history in sidebar
        function renderChatHistory(data) {
            let html = '';

            if (data.today && data.today.length > 0) {
                html += '<div class="chat-section-title">Today</div>';
                data.today.forEach(chat => {
                    html += createChatItemHTML(chat);
                });
            }

            if (data.previous_7_days && data.previous_7_days.length > 0) {
                html += '<div class="chat-section-title">Previous 7 Days</div>';
                data.previous_7_days.forEach(chat => {
                    html += createChatItemHTML(chat);
                });
            }

            if (data.previous_30_days && data.previous_30_days.length > 0) {
                html += '<div class="chat-section-title">Previous 30 Days</div>';
                data.previous_30_days.forEach(chat => {
                    html += createChatItemHTML(chat);
                });
            }

            chatList.innerHTML = html;

            // Attach event listeners
            document.querySelectorAll('.chat-item').forEach(item => {
                item.addEventListener('click', function(e) {
                    if (!e.target.closest('.chat-item-delete')) {
                        loadChat(this.dataset.chatId);
                    }
                });
            });

            document.querySelectorAll('.chat-item-delete').forEach(btn => {
                btn.addEventListener('click', function(e) {
                    e.stopPropagation();
                    deleteChat(this.dataset.chatId);
                });
            });
        }

        // Create HTML for a single chat item
        function createChatItemHTML(chat) {
            const isActive = chat.id === currentChatId ? 'active' : '';
            return `
                <div class="chat-item ${isActive}" data-chat-id="${chat.id}">
                    <span class="chat-item-text">${chat.title}</span>
                    <button class="chat-item-delete" data-chat-id="${chat.id}">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <path d="M3 6h18M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2"/>
                        </svg>
                    </button>
                </div>
            `;
        }

        // Create new chat
        async function createNewChat() {
            try {
                const response = await fetch('/api/chats/new', { method: 'POST' });
                const data = await response.json();
                currentChatId = data.id;
                
                // Clear messages and show welcome
                messagesWrapper.innerHTML = `
                    <div class="welcome-message">
                        <h2>What's on your mind today?</h2>
                        <p>I can help you create, update, and delete measures and calculated columns in your Power BI models.</p>
                        
                        <div class="capabilities">
                            <div class="capability-card">
                                <h3>📊 Create Measures</h3>
                                <p>Define new DAX measures for your data model with intelligent suggestions</p>
                            </div>
                            <div class="capability-card">
                                <h3>✏️ Update Measures</h3>
                                <p>Modify existing measure definitions and optimize DAX formulas</p>
                            </div>
                            <div class="capability-card">
                                <h3>🗑️ Delete Measures</h3>
                                <p>Remove measures you no longer need from your model</p>
                            </div>
                            <div class="capability-card">
                                <h3>➕ Calculated Columns</h3>
                                <p>Create new calculated columns in tables with proper context</p>
                            </div>
                        </div>
                    </div>
                `;
                
                hasMessages = false;
                
                // Set session on websocket
                if (ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({
                        type: 'set_session',
                        session_id: currentChatId
                    }));
                }
                
                // Refresh history
                await loadChatHistory();
            } catch (error) {
                console.error('Failed to create new chat:', error);
            }
        }

        // Load a specific chat
        async function loadChat(chatId) {
            try {
                const response = await fetch(`/api/chats/${chatId}`);
                const chat = await response.json();
                
                if (chat.error) {
                    console.error('Chat not found');
                    return;
                }
                
                currentChatId = chatId;
                hasMessages = chat.messages.length > 0;
                
                // Clear and reload messages
                messagesWrapper.innerHTML = '';
                chat.messages.forEach(msg => {
                    addMessage(msg.content, msg.role, false);
                });
                
                // Set session on websocket
                if (ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({
                        type: 'set_session',
                        session_id: currentChatId
                    }));
                }
                
                // Refresh history to update active state
                await loadChatHistory();
            } catch (error) {
                console.error('Failed to load chat:', error);
            }
        }

        // Delete a chat
        async function deleteChat(chatId) {
            if (!confirm('Are you sure you want to delete this chat?')) {
                return;
            }
            
            try {
                await fetch(`/api/chats/${chatId}`, { method: 'DELETE' });
                
                if (currentChatId === chatId) {
                    await createNewChat();
                } else {
                    await loadChatHistory();
                }
            } catch (error) {
                console.error('Failed to delete chat:', error);
            }
        }

        // New chat functionality
        newChatBtn.addEventListener('click', () => {
            createNewChat();
        });

        // Auto-resize textarea
        messageInput.addEventListener('input', function() {
            this.style.height = 'auto';
            this.style.height = (this.scrollHeight) + 'px';
        });

        function escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }

        function parseTable(content) {
            const lines = content.split('\\n');
            let result = '';
            let i = 0;
            
            while (i < lines.length) {
                const pipeCount = (lines[i].match(/\\|/g) || []).length;
                
                if (pipeCount >= 2) {
                    let tableStart = i;
                    let tableEnd = i;
                    
                    for (let j = i + 1; j < lines.length; j++) {
                        const currentPipes = (lines[j].match(/\\|/g) || []).length;
                        if (currentPipes >= 2) {
                            tableEnd = j;
                        } else {
                            break;
                        }
                    }
                    
                    const tableLines = lines.slice(tableStart, tableEnd + 1);
                    const rows = tableLines
                        .filter(line => line.trim() && !line.match(/^[\\s\\|\\-\\:]+$/))
                        .map(line => 
                            line.split('|')
                                .map(cell => cell.trim())
                                .filter(cell => cell !== '')
                        );
                    
                    if (rows.length > 0) {
                        result += '<table>';
                        result += '<thead><tr>';
                        rows[0].forEach(cell => {
                            result += '<th>' + escapeHtml(cell) + '</th>';
                        });
                        result += '</tr></thead>';
                        
                        if (rows.length > 1) {
                            result += '<tbody>';
                            for (let rowIdx = 1; rowIdx < rows.length; rowIdx++) {
                                result += '<tr>';
                                rows[rowIdx].forEach(cell => {
                                    result += '<td>' + escapeHtml(cell) + '</td>';
                                });
                                result += '</tr>';
                            }
                            result += '</tbody>';
                        }
                        
                        result += '</table>\\n\\n';
                    }
                    
                    i = tableEnd + 1;
                } else {
                    if (lines[i].trim()) {
                        result += lines[i] + '\\n';
                    } else {
                        result += '\\n';
                    }
                    i++;
                }
            }
            
            return result.trim();
        }

        function connect() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const wsUrl = protocol + '//' + window.location.host + '/ws';
            
            console.log('Connecting to:', wsUrl);
            
            try {
                ws = new WebSocket(wsUrl);

                ws.onopen = function() {
                    console.log('Connected to server');
                    isConnected = true;
                    reconnectAttempts = 0;
                    statusIndicator.className = 'status-indicator connected';
                    sendButton.disabled = false;
                    
                    // Set current session if exists
                    if (currentChatId) {
                        ws.send(JSON.stringify({
                            type: 'set_session',
                            session_id: currentChatId
                        }));
                    }
                };

                ws.onmessage = function(event) {
                    console.log('Received message');
                    const data = JSON.parse(event.data);
                    
                    if (data.type === 'response') {
                        removeTypingIndicator();
                        addMessage(data.content, 'assistant');
                        loadChatHistory(); // Refresh history
                    } else if (data.type === 'error') {
                        removeTypingIndicator();
                        addMessage('❌ Error: ' + data.content, 'assistant');
                    }
                };

                ws.onerror = function(error) {
                    console.error('WebSocket error:', error);
                    statusIndicator.className = 'status-indicator error';
                };

                ws.onclose = function() {
                    console.log('Disconnected from server');
                    isConnected = false;
                    sendButton.disabled = true;
                    statusIndicator.className = 'status-indicator error';
                    
                    if (reconnectAttempts < maxReconnectAttempts) {
                        reconnectAttempts++;
                        setTimeout(connect, 3000);
                    }
                };
            } catch (error) {
                console.error('Error creating WebSocket:', error);
                statusIndicator.className = 'status-indicator error';
            }
        }

        function hideWelcome() {
            if (!hasMessages) {
                const welcome = messagesWrapper.querySelector('.welcome-message');
                if (welcome) {
                    welcome.style.display = 'none';
                }
                hasMessages = true;
            }
        }

        function addMessage(content, sender, shouldScroll = true) {
            hideWelcome();
            
            const messageDiv = document.createElement('div');
            messageDiv.className = 'message ' + sender;
            
            const messageContent = document.createElement('div');
            messageContent.className = 'message-content';
            
            const avatar = document.createElement('div');
            avatar.className = 'avatar';
            avatar.textContent = sender === 'user' ? 'U' : 'AI';
            
            const textDiv = document.createElement('div');
            textDiv.className = 'message-text';
            
            if (sender === 'assistant' && content.includes('|')) {
                const parsed = parseTable(content);
                if (parsed !== content) {
                    textDiv.innerHTML = parsed;
                } else {
                    textDiv.textContent = content;
                }
            } else {
                textDiv.textContent = content;
            }
            
            messageContent.appendChild(avatar);
            messageContent.appendChild(textDiv);
            messageDiv.appendChild(messageContent);
            
            messagesWrapper.appendChild(messageDiv);
            
            if (shouldScroll) {
                chatContainer.scrollTop = chatContainer.scrollHeight;
            }
        }

        function addTypingIndicator() {
            hideWelcome();
            
            const messageDiv = document.createElement('div');
            messageDiv.className = 'message assistant';
            messageDiv.id = 'typingIndicator';
            
            const messageContent = document.createElement('div');
            messageContent.className = 'message-content';
            
            const avatar = document.createElement('div');
            avatar.className = 'avatar';
            avatar.textContent = 'AI';
            
            const typingDiv = document.createElement('div');
            typingDiv.className = 'typing-indicator active';
            typingDiv.innerHTML = '<span></span><span></span><span></span>';
            
            const textDiv = document.createElement('div');
            textDiv.className = 'message-text';
            textDiv.appendChild(typingDiv);
            
            messageContent.appendChild(avatar);
            messageContent.appendChild(textDiv);
            messageDiv.appendChild(messageContent);
            
            messagesWrapper.appendChild(messageDiv);
            chatContainer.scrollTop = chatContainer.scrollHeight;
        }

        function removeTypingIndicator() {
            const indicator = document.getElementById('typingIndicator');
            if (indicator) {
                indicator.remove();
            }
        }

        function sendMessage() {
            const message = messageInput.value.trim();
            
            if (message && isConnected) {
                // Create new chat if needed
                if (!currentChatId) {
                    createNewChat().then(() => {
                        // After creating new chat, send the message
                        addMessage(message, 'user');
                        addTypingIndicator();
                        
                        try {
                            ws.send(JSON.stringify({
                                type: 'message',
                                content: message
                            }));
                            
                            messageInput.value = '';
                            messageInput.style.height = 'auto';
                        } catch (error) {
                            console.error('Error sending message:', error);
                            removeTypingIndicator();
                            addMessage('Failed to send message. Please try again.', 'assistant');
                        }
                    });
                } else {
                    addMessage(message, 'user');
                    addTypingIndicator();
                    
                    try {
                        ws.send(JSON.stringify({
                            type: 'message',
                            content: message
                        }));
                        
                        messageInput.value = '';
                        messageInput.style.height = 'auto';
                    } catch (error) {
                        console.error('Error sending message:', error);
                        removeTypingIndicator();
                        addMessage('Failed to send message. Please try again.', 'assistant');
                    }
                }
            }
        }

        sendButton.addEventListener('click', sendMessage);
        
        messageInput.addEventListener('keypress', function(e) {
            if (e.key === 'Enter' && !e.shiftKey && !sendButton.disabled) {
                e.preventDefault();
                sendMessage();
            }
        });

        // Initialize
        connect();
        createNewChat();
    </script>
</body>
</html>
"""

# --------------------------------------------------
# Lifespan context manager
# --------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage the lifespan of the application"""
    global agent_instance, mcp_session, agent_initialized, initialization_error
    
    print("=" * 70)
    print("🚀 Starting MCP Chatbot Server...")
    print("=" * 70)
    
    # Startup
    try:
        # Check environment variables
        POWERBI_MCP_EXE = os.environ.get("POWERBI_MCP_EXE")
        MCP_ARGS = os.environ.get("MCP_PBI_ARGS", "").split()
        OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
        
        if not POWERBI_MCP_EXE:
            raise ValueError("POWERBI_MCP_EXE environment variable not set")
        if not OPENAI_API_KEY:
            raise ValueError("OPENAI_API_KEY environment variable not set")
        
        print(f"✓ Environment variables loaded")
        print(f"  - POWERBI_MCP_EXE: {POWERBI_MCP_EXE}")
        print(f"  - MCP_ARGS: {MCP_ARGS}")
        print(f"  - OPENAI_API_KEY: {'*' * 10}{OPENAI_API_KEY[-4:] if len(OPENAI_API_KEY) > 4 else '****'}")
        
        # Start MCP server
        print("\n📡 Starting MCP server...")
        server_params = StdioServerParameters(
            command=POWERBI_MCP_EXE,
            args=MCP_ARGS
        )

        # Keep the context managers alive for the entire app lifespan
        stdio_ctx = stdio_client(server_params)
        read, write = await stdio_ctx.__aenter__()
        
        session_ctx = ClientSession(read, write)
        mcp_session = await session_ctx.__aenter__()
        await mcp_session.initialize()
        print("✓ MCP server initialized")

        # Load MCP tools with truncated descriptions
        print("\n🔧 Loading MCP tools...")
        all_tools = await load_mcp_tools(mcp_session)
        
        # Truncate tool descriptions to save tokens
        tools = []
        for tool in all_tools:
            if hasattr(tool, 'description') and tool.description:
                # Keep only first 100 characters of description
                tool.description = tool.description[:100] + "..." if len(tool.description) > 100 else tool.description
            tools.append(tool)
        
        print(f"✓ Loaded {len(tools)} tools with optimized descriptions")

        # Initialize LLM
        print("\n🤖 Initializing LLM...")
        llm = ChatOpenAI(
            model="gpt-4o",
            temperature=0.2,
            api_key=OPENAI_API_KEY
        )
        print("✓ LLM initialized")

        # Create agent
        print("\n🎯 Creating agent...")
        checkpointer = InMemorySaver()
        agent_instance = create_react_agent(
            model=llm,
            tools=tools,
            prompt=AGENT_SYSTEM_PROMPT,
            checkpointer=checkpointer
        )
        print("✓ Agent created")
        
        agent_initialized = True
        
        print("\n" + "=" * 70)
        print("✅ MCP Chatbot Server started successfully!")
        print("=" * 70)
        print("🌐 Access the chatbot at:")
        print("   - http://localhost:8005")
        print("   - http://127.0.0.1:8005")
        print("=" * 70)
        
        yield  # Server is running
        
        # Shutdown
        print("\n🛑 Shutting down MCP server...")
        await session_ctx.__aexit__(None, None, None)
        await stdio_ctx.__aexit__(None, None, None)
        print("✓ MCP server stopped")
        
    except Exception as e:
        initialization_error = str(e)
        agent_initialized = False
        print("\n" + "=" * 70)
        print("❌ Failed to initialize agent:")
        print(f"   {e}")
        print("=" * 70)
        print("\n⚠️  Server is running but agent is not available.")
        print("   Check your environment variables and configuration.")
        print("=" * 70)
        
        yield  # Still start the server even if agent fails

# --------------------------------------------------
# FastAPI App with lifespan
# --------------------------------------------------
app = FastAPI(title="Power BI MCP Chatbot", lifespan=lifespan)

# --------------------------------------------------
# Routes
# --------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def get_ui():
    """Serve the main UI"""
    return HTML_CONTENT

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "ok" if agent_initialized else "error",
        "agent_initialized": agent_initialized,
        "initialization_error": initialization_error
    }

@app.get("/api/chats")
async def get_chats():
    """Get all chat sessions organized by time"""
    from datetime import datetime, timedelta
    
    now = datetime.now()
    today = []
    previous_7_days = []
    previous_30_days = []
    
    for session_id, chat_data in chat_sessions.items():
        created_at = datetime.fromisoformat(chat_data['created_at'])
        age = now - created_at
        
        chat_item = {
            'id': session_id,
            'title': chat_data['title'],
            'created_at': chat_data['created_at'],
            'message_count': len(chat_data['messages'])
        }
        
        if age.days == 0:
            today.append(chat_item)
        elif age.days <= 7:
            previous_7_days.append(chat_item)
        elif age.days <= 30:
            previous_30_days.append(chat_item)
    
    return {
        'today': sorted(today, key=lambda x: x['created_at'], reverse=True),
        'previous_7_days': sorted(previous_7_days, key=lambda x: x['created_at'], reverse=True),
        'previous_30_days': sorted(previous_30_days, key=lambda x: x['created_at'], reverse=True)
    }

@app.post("/api/chats/new")
async def create_new_chat():
    """Create a new chat session"""
    import uuid
    from datetime import datetime
    
    session_id = str(uuid.uuid4())
    chat_sessions[session_id] = {
        'id': session_id,
        'title': 'New Chat',
        'created_at': datetime.now().isoformat(),
        'messages': []
    }
    
    return {'id': session_id}

@app.get("/api/chats/{session_id}")
async def get_chat(session_id: str):
    """Get a specific chat session"""
    if session_id in chat_sessions:
        return chat_sessions[session_id]
    return {"error": "Chat not found"}

@app.delete("/api/chats/{session_id}")
async def delete_chat(session_id: str):
    """Delete a chat session"""
    if session_id in chat_sessions:
        del chat_sessions[session_id]
        return {"success": True}
    return {"error": "Chat not found"}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for chat"""
    await websocket.accept()
    
    # Check if agent is initialized
    if not agent_initialized:
        await websocket.send_json({
            "type": "error",
            "content": f"Agent not initialized. Error: {initialization_error or 'Unknown error'}"
        })
        await websocket.close()
        return
    
    current_session_id = None
    
    try:
        while True:
            # Receive message from client
            data = await websocket.receive_json()
            
            if data.get("type") == "set_session":
                current_session_id = data.get("session_id")
                
            elif data.get("type") == "message":
                user_input = data.get("content", "").strip()
                
                if not user_input or not current_session_id:
                    continue
                
                # Initialize session if new
                if current_session_id not in chat_sessions:
                    from datetime import datetime
                    chat_sessions[current_session_id] = {
                        'id': current_session_id,
                        'title': user_input[:50] + ('...' if len(user_input) > 50 else ''),
                        'created_at': datetime.now().isoformat(),
                        'messages': []
                    }
                
                # Store user message
                chat_sessions[current_session_id]['messages'].append({
                    'role': 'user',
                    'content': user_input
                })
                
                try:
                    # Run agent
                    print(f"[DEBUG] Processing user input: {user_input[:50]}...")
                    response = await run_agent_once(
                        agent_instance, 
                        user_input, 
                        thread_id=current_session_id
                    )
                    print(f"[DEBUG] Agent response: {response[:100]}...")
                    
                    # Store assistant message
                    chat_sessions[current_session_id]['messages'].append({
                        'role': 'assistant',
                        'content': response
                    })
                    
                    # Update title if this is the first message
                    if len(chat_sessions[current_session_id]['messages']) == 2:
                        chat_sessions[current_session_id]['title'] = user_input[:50] + ('...' if len(user_input) > 50 else '')
                    
                    # Send response back
                    await websocket.send_json({
                        "type": "response",
                        "content": response
                    })
                    
                except Exception as e:
                    print(f"Error running agent: {e}")
                    import traceback
                    traceback.print_exc()
                    await websocket.send_json({
                        "type": "error",
                        "content": str(e)
                    })
                    
    except WebSocketDisconnect:
        print(f"Client disconnected (session: {current_session_id})")
    except Exception as e:
        print(f"WebSocket error: {e}")

# --------------------------------------------------
# Agent Runner
# --------------------------------------------------
async def run_agent_once(agent, user_input, thread_id="powerbi-chat-session"):
    """Run agent with intelligent search and context"""
    
    # Detect intent
    intent = detect_user_intent(user_input)
    info = parse_measure_request(user_input)
    
    # Build context hints for the agent
    hints = []
    
    if intent != 'unknown':
        hints.append(f"[Intent: {intent.upper()}]")
    
    if info['operation']:
        hints.append(f"[Operation: {info['operation']}]")
    
    if info['has_explicit_syntax']:
        hints.append(f"[Explicit: {info['table']}[{info['column']}]]")
        hints.append("[High confidence - execute directly]")
    else:
        # Suggest the agent should search
        hints.append("[Search for table/column names before asking user]")
    
    # Enhance input with context
    enhanced = user_input + "\n" + " ".join(hints)
    
    messages = [HumanMessage(content=enhanced)]
    
    try:
        result = await agent.ainvoke(
            {"messages": messages},
            config={
                "configurable": {"thread_id": thread_id},
                "recursion_limit": 50
            }
        )
        
        print(f"[DEBUG] Intent: {intent}, Has explicit: {info['has_explicit_syntax']}")
        
        # Extract response
        all_messages = result.get("messages", [])
        
        if not all_messages:
            return "No response received."
        
        for msg in reversed(all_messages):
            if type(msg).__name__ in ['AIMessage', 'AssistantMessage']:
                content = msg.content
                
                if isinstance(content, str) and content.strip():
                    # Clean hints from response
                    cleaned = content
                    for hint in hints:
                        cleaned = cleaned.replace(hint, "")
                    return cleaned.strip()
                
                elif isinstance(content, list):
                    texts = []
                    for item in content:
                        if isinstance(item, dict) and 'text' in item:
                            texts.append(item['text'])
                        elif isinstance(item, str):
                            texts.append(item)
                    
                    if texts:
                        result_text = '\n'.join(t for t in texts if t.strip())
                        for hint in hints:
                            result_text = result_text.replace(hint, "")
                        return result_text.strip()
        
        # Fallback
        last_msg = all_messages[-1]
        if hasattr(last_msg, 'content'):
            content = last_msg.content
            if isinstance(content, str):
                return content if content.strip() else "✓ Operation completed successfully."
            elif isinstance(content, list):
                text = ' '.join([str(item.get('text', item)) if isinstance(item, dict) else str(item) for item in content])
                return text if text.strip() else "✓ Operation completed successfully."
        
        return "✓ Operation completed successfully."
        
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
        
        error = str(e).lower()
        if "not found" in error or "does not exist" in error:
            return "❌ Table/column not found. Please verify the exact name in your Power BI model."
        elif "syntax" in error:
            return "❌ DAX syntax error. Please check the formula."
        elif "context" in error or "token" in error:
            return "❌ Request too long. Please simplify or break into smaller parts."
        else:
            return f"❌ Error: {str(e)}"

# --------------------------------------------------
# Entry point
# --------------------------------------------------
if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("🎬 Starting FastAPI server...")
    print("=" * 70)
    
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=8005,
        log_level="info"
    )