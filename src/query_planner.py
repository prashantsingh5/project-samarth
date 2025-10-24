"""Query Planner: Parse natural language questions using Gemini AI."""

import os
import json
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import google.generativeai as genai


@dataclass
class QueryPlan:
    """Structured representation of a query execution plan."""
    intent: str
    metric: str
    states: List[str]
    crops: Optional[List[str]] = None
    districts: Optional[List[str]] = None
    year_start: Optional[int] = None
    year_end: Optional[int] = None
    season: Optional[str] = None
    aggregation: str = "sum"
    multi_part: bool = False
    sub_queries: Optional[List[str]] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class QueryPlanner:
    """Use Gemini AI to parse natural language questions into structured query plans."""

    def __init__(self, api_key: str):
        genai.configure(api_key=api_key)  # type: ignore
        self.model = genai.GenerativeModel('gemini-2.5-flash')  # type: ignore

    def parse_question(self, question: str) -> QueryPlan:
        """
        Parse natural language question into structured query plan.

        Args:
            question: User's natural language question

        Returns:
            QueryPlan with extracted intent and entities
        """
        # Create prompt for Gemini
        prompt = f"""You are a query parser for an agricultural data Q&A system with data from 1901-2017.

Parse this question into a structured JSON format:

Question: "{question}"

Extract:
1. intent: one of [comparison, extremes, trends, correlation, multi_part]
2. metric: one of [production, rainfall, price, multi]
3. states: list of Indian state names (use canonical names: Karnataka, Tamil Nadu, etc.)
4. crops: list of crop names (if mentioned), use canonical names: Rice, Wheat, Maize, Cotton, etc.
5. districts: list of district names (if mentioned)
6. year_start: start year (integer). If "last N years", calculate from 2017 (data ends at 2017). Example: "last 5 years" = 2013
7. year_end: end year (integer, if mentioned or same as year_start). For "last N years", use 2017
8. season: one of [Kharif, Rabi, Summer, Whole Year] (if mentioned)
9. aggregation: one of [sum, avg, max, min] (default: sum)
10. multi_part: true if question has multiple parts (e.g., "compare rainfall AND list crops"), false otherwise
11. sub_queries: array of sub-question descriptions if multi_part is true

Rules:
- If question asks "which is higher/more" between two STATES, intent is "comparison"
- If question asks "highest/lowest/maximum/minimum" in a district/state, intent is "extremes"
- If question asks for highest in one state AND lowest in another state, intent is "extremes" with both states
- If question asks "trend/change/growth", intent is "trends"
- If question asks "correlation/relationship/impact", intent is "correlation"
- If question has multiple parts (AND, "in parallel", "also show"), intent is "multi_part" and metric is "multi"
- For "last N years", calculate from 2017 (our data ends in 2017)
- For "most recent year", use 2017
- For "top N crops", extract N and add to sub_queries
- Use only canonical state/crop names
- Return ONLY valid JSON, no markdown or explanation

Example outputs:

Simple comparison:
{{
    "intent": "comparison",
    "metric": "production",
    "states": ["Karnataka", "Tamil Nadu"],
    "crops": ["Rice"],
    "year_start": 2015,
    "year_end": 2015,
    "season": null,
    "aggregation": "sum",
    "multi_part": false,
    "sub_queries": null
}}

Multi-part query:
{{
    "intent": "multi_part",
    "metric": "multi",
    "states": ["Karnataka", "Tamil Nadu"],
    "crops": null,
    "year_start": 2013,
    "year_end": 2017,
    "season": "Kharif",
    "aggregation": "avg",
    "multi_part": true,
    "sub_queries": [
        "Compare average annual rainfall",
        "List top 5 Kharif crops by production volume"
    ]
}}

Now parse the question and return JSON:"""

        try:
            # Call Gemini API
            response = self.model.generate_content(prompt)
            response_text = response.text.strip()

            # Remove markdown code blocks if present
            if response_text.startswith("```json"):
                response_text = response_text.split("```json")[1].split("```")[0].strip()
            elif response_text.startswith("```"):
                response_text = response_text.split("```")[1].split("```")[0].strip()

            # Parse JSON response
            parsed = json.loads(response_text)

            # Create QueryPlan
            plan = QueryPlan(
                intent=parsed.get("intent", "comparison"),
                metric=parsed.get("metric", "production"),
                states=parsed.get("states", []),
                crops=parsed.get("crops"),
                districts=parsed.get("districts"),
                year_start=parsed.get("year_start"),
                year_end=parsed.get("year_end"),
                season=parsed.get("season"),
                aggregation=parsed.get("aggregation", "sum"),
                multi_part=parsed.get("multi_part", False),
                sub_queries=parsed.get("sub_queries")
            )

            return plan

        except Exception as e:
            # Fallback: return a basic plan if parsing fails
            print(f"Warning: Gemini parsing failed: {e}")
            return QueryPlan(
                intent="comparison",
                metric="production",
                states=["Karnataka"],
                year_start=2010,
                year_end=2010
            )

    def validate_plan(self, plan: QueryPlan) -> bool:
        """
        Validate query plan has required fields.

        Args:
            plan: QueryPlan to validate

        Returns:
            True if valid, False otherwise
        """
        # Check required fields
        if not plan.intent or plan.intent not in ["comparison", "extremes", "trends", "correlation", "multi_part"]:
            return False

        if not plan.metric or plan.metric not in ["production", "rainfall", "price"]:
            return False

        if not plan.states or len(plan.states) == 0:
            return False

        return True
