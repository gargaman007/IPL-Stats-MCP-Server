
import json
import logging
import os
import sqlite3
import sys

import google.generativeai as genai
from mcp.server.fastmcp import FastMCP

# --- Best Practice: Configure Logging to stderr ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)

# --- Initialize FastMCP Server ---
mcp = FastMCP("ipl_stats")

# --- Configuration ---
try:
    # It's recommended to use environment variables for security
    # genai.configure(api_key=os.environ["GEMINI_API_KEY"])

    # Using the hardcoded key from your example for consistency
    genai.configure(api_key="Google_API_Key")  # Replace with your actual API key
except KeyError:
    logging.error("FATAL: GEMINI_API_KEY environment variable not set.")
    sys.exit("API key not found. Please set the GEMINI_API_KEY environment variable.")

DB_FILE = "ipl.db"

# --- Database Schema (Unchanged) ---
SCHEMA = """
CREATE TABLE teams (
    id INTEGER NOT NULL,
    name VARCHAR,
    PRIMARY KEY (id),
    UNIQUE (name)
);
CREATE TABLE players (
    id VARCHAR NOT NULL,
    name VARCHAR,
    PRIMARY KEY (id)
);
CREATE TABLE matches (
    id VARCHAR NOT NULL,
    season VARCHAR,
    venue VARCHAR,
    city VARCHAR,
    match_date DATE,
    match_type VARCHAR,
    balls_per_over INTEGER,
    team1_id INTEGER,
    team2_id INTEGER,
    toss_winner_id INTEGER,
    toss_decision VARCHAR,
    winner_id INTEGER,
    outcome_result VARCHAR,
    outcome_margin INTEGER,
    player_of_match_id VARCHAR,
    officials JSON,
    PRIMARY KEY (id),
    FOREIGN KEY(team1_id) REFERENCES teams (id),
    FOREIGN KEY(team2_id) REFERENCES teams (id),
    FOREIGN KEY(toss_winner_id) REFERENCES teams (id),
    FOREIGN KEY(winner_id) REFERENCES teams (id),
    FOREIGN KEY(player_of_match_id) REFERENCES players (id)
);
CREATE TABLE innings (
    id INTEGER NOT NULL,
    match_id VARCHAR,
    inning_number INTEGER,
    batting_team_id INTEGER,
    PRIMARY KEY (id),
    FOREIGN KEY(match_id) REFERENCES matches (id),
    FOREIGN KEY(batting_team_id) REFERENCES teams (id)
);
CREATE TABLE deliveries (
    id INTEGER NOT NULL,
    inning_id INTEGER,
    over FLOAT,
    batter_id VARCHAR,
    bowler_id VARCHAR,
    non_striker_id VARCHAR,
    runs_batter INTEGER,
    runs_extras INTEGER,
    runs_total INTEGER,
    extras_type VARCHAR,
    is_wicket INTEGER,
    player_out_id VARCHAR,
    wicket_kind VARCHAR,
    wicket_fielders JSON,
    PRIMARY KEY (id),
    FOREIGN KEY(inning_id) REFERENCES innings (id),
    FOREIGN KEY(batter_id) REFERENCES players (id),
    FOREIGN KEY(bowler_id) REFERENCES players (id),
    FOREIGN KEY(non_striker_id) REFERENCES players (id),
    FOREIGN KEY(player_out_id) REFERENCES players (id)
);
"""

# --- NEW: Improved Prompt with Few-Shot Examples ---
IMPROVED_PROMPT_TEMPLATE = """
You are an expert SQLite analyst. Given the following SQLite database schema, your task is to convert a natural language question into a single, valid SQLite query.

**Database Schema:**
```sql
{SCHEMA}
```

Key Instructions & Rules:
- Always JOIN for Names: When asked for a team or player, you MUST JOIN the teams or players table to return the name, not just the ID.
- Handle Player Name Variations: Be flexible with names. For example, "Virat Kohli" should match "V Kohli". Use LIKE for flexibility (e.g., p.name LIKE '%Kohli%').
- Interpret Ambiguous Terms:
    - "Highest Score" means the highest total runs by a team in a single innings.
    - "Best Bowling Figures" means the most wickets taken in a single match, with the fewest runs conceded as a tie-breaker.
    - "Powerplay" refers to the first 6 overs. In the database, this is WHERE d.over < 6.
    - "Closest Match" means the smallest outcome_margin for matches won by runs.
- Only return the SQL query and nothing else. Do not add explanations or markdown.

Query Examples:

Question: "Which team won the most matches?"
SQL: SELECT t.name, COUNT(m.winner_id) AS wins FROM teams t JOIN matches m ON t.id = m.winner_id GROUP BY t.name ORDER BY wins DESC LIMIT 1;

Question: "Highest total score?"
SQL: SELECT t.name as team, SUM(d.runs_total) as total_score FROM deliveries d JOIN innings i ON d.inning_id = i.id JOIN teams t ON i.batting_team_id = t.id GROUP BY i.id ORDER BY total_score DESC LIMIT 1;

Question: "Show me Virat Kohli's batting stats"
SQL: SELECT p.name, SUM(d.runs_batter) AS total_runs, COUNT(CASE WHEN d.is_wicket = 1 AND d.player_out_id = p.id THEN 1 END) AS dismissals FROM players p JOIN deliveries d ON p.id = d.batter_id WHERE p.name LIKE '%V Kohli%' GROUP BY p.name;

Question: "Who has the best bowling figures in a single match?"
SQL: SELECT p.name, i.match_id, COUNT(CASE WHEN d.is_wicket = 1 AND d.wicket_kind != 'run out' THEN 1 END) AS wickets, SUM(d.runs_total) AS runs_conceded FROM players p JOIN deliveries d ON p.id = d.bowler_id JOIN innings i ON d.inning_id = i.id GROUP BY p.name, i.match_id ORDER BY wickets DESC, runs_conceded ASC LIMIT 1;

Question: "Show me the scorecard for the match between MI and CSK"
SQL: SELECT i.inning_number, t_bat.name as batting_team, p_bat.name as batter, SUM(d.runs_batter) as runs, COUNT(d.over) as balls_faced, p_bowl.name as bowler, COUNT(CASE WHEN d.is_wicket = 1 AND d.wicket_kind != 'run out' AND d.bowler_id = p_bowl.id THEN 1 END) as wickets_taken FROM deliveries d JOIN innings i ON d.inning_id = i.id JOIN matches m ON i.match_id = m.id JOIN teams t1 ON m.team1_id = t1.id JOIN teams t2 ON m.team2_id = t2.id JOIN teams t_bat ON i.batting_team_id = t_bat.id JOIN players p_bat ON d.batter_id = p_bat.id JOIN players p_bowl ON d.bowler_id = p_bowl.id WHERE (t1.name LIKE '%MI%' AND t2.name LIKE '%CSK%') OR (t1.name LIKE '%CSK%' AND t2.name LIKE '%MI%') GROUP BY i.inning_number, t_bat.name, p_bat.name, p_bowl.name ORDER BY i.inning_number, runs DESC;

Now, generate the SQL query for the following question.
Question: "{question}"
SQL Query:
"""

# --- Helper Functions ---
async def get_sql_from_llm(question: str) -> str | None:
    """Uses Gemini to convert a natural language question into a SQL query."""
    model = genai.GenerativeModel('gemini-1.5-flash-latest')
    prompt = IMPROVED_PROMPT_TEMPLATE.format(SCHEMA=SCHEMA, question=question)

    try:
        response = await model.generate_content_async(prompt)
        sql_query = response.text.strip().replace("sql", "").replace("```", "").replace("\n", " ").strip()
        return sql_query
    except Exception as e:
        logging.error(f"Error generating SQL from LLM: {e}")
        return None

def execute_sql_query(query: str) -> list | dict:
    """Executes a SQL query on the database and returns the result."""
    try:
        if not os.path.exists(DB_FILE):
            return {"error": f"Database file '{DB_FILE}' not found. Please run the data loading script first."}

        conn = sqlite3.connect(DB_FILE)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(query)

        if cursor.description is None:
            conn.close()
            return []

        column_names = [description[0] for description in cursor.description]
        results = [dict(zip(column_names, row)) for row in cursor.fetchall()]
        conn.close()
        return results

    except sqlite3.Error as e:
        logging.error(f"Database error: {e}\nFailed query: {query}")
        return {"error": f"Database Error: {e}", "failed_query": query}

# --- MCP Tool Implementation ---
@mcp.tool()
async def query_ipl_database(question: str) -> str:
    """
    Answers natural language questions about IPL cricket data by querying a SQL database.
    Use this for any questions about matches, players, scores, teams, and stats from the IPL.
    Args:
        question: The user's question about IPL cricket. E.g., "Who won the most matches?"
    """
    logging.info(f"Received question for tool: {question}")
    sql_query = await get_sql_from_llm(question)

    if not sql_query:
        return "Sorry, I had trouble understanding that question. Could you rephrase it?"

    logging.info(f"Generated SQL: {sql_query}")
    results = execute_sql_query(sql_query)

    if isinstance(results, dict) and "error" in results:
        return f"A database error occurred. Details: {results['error']}"

    if not results:
        return "I couldn't find any data for your question."

    if len(results) == 1 and len(results[0]) == 1:
        # If the result is just a single value (e.g., a count or a name), return it directly.
        single_value = list(results[0].values())[0]
        return f"The result is: {single_value}"

    # For more complex data, return the formatted JSON table.
    return json.dumps(results, indent=2)

# --- Running the Server ---
if __name__ == "__main__":
    logging.info("Starting IPL MCP Server, waiting for client connection...")
    mcp.run(transport='stdio')
