"""
Answer Generator: Transform raw query results into natural language answers.
"""

import json
from typing import Dict, Any
import google.generativeai as genai
from .query_planner import QueryPlan


class AnswerGenerator:
    """Generate natural language answers from query results using AI."""

    def __init__(self, api_key: str):
        genai.configure(api_key=api_key)  # type: ignore
        self.model = genai.GenerativeModel('gemini-2.5-flash')  # type: ignore

    def generate_answer(
        self,
        original_question: str,
        query_plan: QueryPlan,
        raw_answer: str,
        data_summary: Dict[str, Any],
        metadata: Dict[str, Any]
    ) -> str:
        """Generate natural language answer from query results."""
        prompt = self._build_prompt(
            original_question,
            query_plan,
            raw_answer,
            data_summary,
            metadata
        )

        try:
            response = self.model.generate_content(prompt)  # type: ignore
            
            if response and response.text:
                enhanced_answer = response.text.strip()
                
                if metadata.get('sources'):
                    enhanced_answer += "\n\n" + self._format_citations(metadata['sources'])
                
                return enhanced_answer
            else:
                return raw_answer
                
        except Exception as e:
            print(f"⚠️  AI answer generation failed: {e}")
            print("   Falling back to raw data output")
            return raw_answer

    def _build_prompt(
        self,
        question: str,
        plan: QueryPlan,
        raw_answer: str,
        data_summary: Dict[str, Any],
        metadata: Dict[str, Any]
    ) -> str:
        """Build the prompt for AI answer generation."""
        
        prompt = f"""You are an expert data analyst helping users understand agricultural and climate data from India.

USER'S QUESTION:
{question}

QUERY CONTEXT:
- Intent: {plan.intent}
- Region: {', '.join(plan.states)}
- Time Period: {plan.year_start or 'All years'} to {plan.year_end or 'Latest'}
{f"- Crops: {', '.join(plan.crops)}" if plan.crops else ""}
{f"- Season: {plan.season}" if plan.season else ""}

RAW DATA RETRIEVED:
{raw_answer}

ADDITIONAL CONTEXT:
{json.dumps(data_summary, indent=2)}

YOUR TASK:
Generate a clear, insightful answer that:

1. **DIRECTLY ANSWERS THE QUESTION** - Address what the user asked
2. **INTERPRET THE DATA** - Explain what the numbers mean, don't just repeat them
3. **PROVIDE CONTEXT** - Add relevant agricultural/climate knowledge (e.g., water requirements, crop characteristics)
4. **SYNTHESIZE INSIGHTS** - Connect different data points (rainfall + production, trends + causes)
5. **BE SPECIFIC** - Use actual numbers from the data, cite specific findings
6. **STRUCTURE CLEARLY** - Use headings, bullet points, emojis for readability

IMPORTANT GUIDELINES:
- If the question asks for "N arguments" or "N reasons", provide exactly that many
- For policy questions, focus on data-backed recommendations
- For comparisons, highlight key differences and their significance
- For trends/correlations, explain potential causes and implications
- Keep tone professional but accessible
- DO NOT make up data - only use what's provided
- DO NOT add citations/sources (those will be added separately)

Generate the answer now:"""

        return prompt

    def _format_citations(self, sources: list) -> str:
        """Format data sources as citations."""
        citation_text = "📚 DATA SOURCES:\n"
        
        for i, source in enumerate(sources, 1):
            citation_text += f"   [{i}] {source.get('name', 'Unknown source')}\n"
            
            if source.get('filters'):
                filters = source['filters']
                filter_parts = []
                
                if filters.get('states'):
                    filter_parts.append(f"States: {', '.join(filters['states'])}")
                if filters.get('crops'):
                    filter_parts.append(f"Crops: {', '.join(filters['crops'])}")
                if filters.get('year_start') and filters.get('year_end'):
                    filter_parts.append(f"Period: {filters['year_start']}-{filters['year_end']}")
                elif filters.get('year'):
                    filter_parts.append(f"Year: {filters['year']}")
                if filters.get('season'):
                    filter_parts.append(f"Season: {filters['season']}")
                
                if filter_parts:
                    citation_text += f"       ({', '.join(filter_parts)})\n"
        
        return citation_text.rstrip()

    def extract_data_summary(self, query_plan: QueryPlan, raw_data: Any, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Extract key data points for summarization."""
        summary = {
            "query_type": query_plan.intent,
            "states": query_plan.states,
            "crops": query_plan.crops,
            "time_range": f"{query_plan.year_start or 'N/A'} to {query_plan.year_end or 'N/A'}"
        }

        if metadata.get('correlation'):
            summary['correlation_coefficient'] = metadata['correlation']
            summary['correlation_strength'] = metadata.get('strength', 'unknown')
        
        if metadata.get('production_trend'):
            summary['production_trend'] = metadata['production_trend']
        
        if metadata.get('rainfall_trend'):
            summary['rainfall_trend'] = metadata['rainfall_trend']
        
        if metadata.get('overlapping_years'):
            summary['data_coverage'] = f"{metadata['overlapping_years']} years"

        return summary
