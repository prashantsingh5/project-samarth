"""
Query Executor: Execute query plans and return analyzed results.

This module uses Phase 1 connectors to fetch data and perform analysis
based on parsed query plans.
"""

import pandas as pd
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from .data_connector import DataGovInConnector
from .query_planner import QueryPlan


@dataclass
class QueryResult:
    """Structured representation of query execution result."""
    answer: str  # Human-readable answer
    data: pd.DataFrame  # Supporting data
    metadata: Dict[str, Any]  # Query metadata and citations
    confidence: float = 1.0  # Confidence score (0-1) based on data quality


class QueryExecutor:
    """Execute query plans and return analyzed results."""

    def __init__(self, connector: DataGovInConnector):
        """
        Initialize executor with data connector.

        Args:
            connector: DataGovInConnector instance from Phase 1
        """
        self.connector = connector

    def _calculate_confidence(self, data_frames: List[pd.DataFrame], plan: QueryPlan) -> float:
        """
        Calculate confidence score based on data quality, sample size, and recency.
        
        Args:
            data_frames: List of DataFrames used in analysis
            plan: Query plan with time range info
            
        Returns:
            Confidence score between 0 and 1
        """
        if not data_frames or all(df.empty for df in data_frames):
            return 0.0
        
        # Extract quality scores from DataFrame metadata (if available)
        quality_scores = []
        for df in data_frames:
            if hasattr(df, 'attrs') and 'quality_report' in df.attrs:
                quality_scores.append(df.attrs['quality_report'].overall_score)
        
        # Average quality score (default to 0.9 if no quality reports)
        avg_quality = sum(quality_scores) / len(quality_scores) if quality_scores else 0.9
        
        # Sample size factor (more data = higher confidence)
        total_records = sum(len(df) for df in data_frames)
        if total_records >= 100:
            sample_factor = 1.0
        elif total_records >= 50:
            sample_factor = 0.95
        elif total_records >= 20:
            sample_factor = 0.85
        elif total_records >= 10:
            sample_factor = 0.75
        else:
            sample_factor = 0.6
        
        # Recency factor (newer data = higher confidence)
        recency_factor = 1.0
        if plan.year_end:
            years_old = 2024 - plan.year_end
            if years_old >= 10:
                recency_factor = 0.7
            elif years_old >= 5:
                recency_factor = 0.85
            elif years_old >= 2:
                recency_factor = 0.95
        
        # Combined confidence (weighted average)
        confidence = (0.5 * avg_quality + 0.3 * sample_factor + 0.2 * recency_factor)
        return round(confidence, 2)

    def execute(self, plan: QueryPlan) -> QueryResult:
        """
        Execute query plan and return result.

        Args:
            plan: QueryPlan from query planner

        Returns:
            QueryResult with answer, data, and citations
        """
        # Route to appropriate handler based on intent
        if plan.intent == "comparison":
            return self._execute_comparison(plan)
        elif plan.intent == "extremes":
            return self._execute_extremes(plan)
        elif plan.intent == "trends":
            return self._execute_trends(plan)
        elif plan.intent == "correlation":
            return self._execute_correlation(plan)
        elif plan.intent == "multi_part":
            return self._execute_multi_part(plan)
        else:
            raise ValueError(f"Unknown intent: {plan.intent}")

    def _execute_comparison(self, plan: QueryPlan) -> QueryResult:
        """Execute comparison query."""
        if plan.metric == "production":
            # Fetch production data for all states
            dfs = []
            for state in plan.states:
                df = self.connector.fetch_production(
                    state=state,
                    crop=plan.crops[0] if plan.crops else None,
                    year=plan.year_start
                )
                dfs.append(df)

            combined = pd.concat(dfs, ignore_index=True)

            # Aggregate by state
            result = combined.groupby('state_name')['production_tonne'].sum().reset_index()
            result = result.sort_values('production_tonne', ascending=False)

            # Generate answer
            if len(result) >= 2:
                top_state = result.iloc[0]['state_name']
                top_value = result.iloc[0]['production_tonne']
                second_state = result.iloc[1]['state_name']
                second_value = result.iloc[1]['production_tonne']
                diff = top_value - second_value
                pct = (diff / second_value * 100) if second_value > 0 else 0

                crop_name = plan.crops[0] if plan.crops else "crops"
                answer = f"{top_state} had higher {crop_name} production in {plan.year_start} with {top_value:,.0f} tonnes, {diff:,.0f} tonnes ({pct:.1f}%) more than {second_state}."
            else:
                answer = "Insufficient data for comparison."

            metadata = {
                "source": "District-wise crop production (data.gov.in)",
                "resource_id": "35be999b-0208-4354-b557-f6ca9a5355de",
                "filters": {"states": plan.states, "crop": plan.crops[0] if plan.crops else None, "year": plan.year_start}
            }

            confidence = self._calculate_confidence(dfs, plan)
            return QueryResult(answer=answer, data=result, metadata=metadata, confidence=confidence)

        elif plan.metric == "rainfall":
            # Fetch rainfall data for all states
            dfs = []
            for state in plan.states:
                df = self.connector.fetch_rainfall_by_state(
                    state=state,
                    year_start=plan.year_start,
                    year_end=plan.year_end or plan.year_start
                )
                dfs.append(df)

            combined = pd.concat(dfs, ignore_index=True)

            # Aggregate by state (average across years)
            result = combined.groupby('state_name')['rainfall_mm'].mean().reset_index()
            result = result.sort_values('rainfall_mm', ascending=False)

            # Generate answer
            if len(result) >= 2:
                top_state = result.iloc[0]['state_name']
                top_value = result.iloc[0]['rainfall_mm']
                second_state = result.iloc[1]['state_name']
                second_value = result.iloc[1]['rainfall_mm']
                diff = top_value - second_value
                pct = (diff / second_value * 100) if second_value > 0 else 0

                year_text = f"{plan.year_start}" if plan.year_start == plan.year_end else f"{plan.year_start}-{plan.year_end}"
                answer = f"{top_state} received higher rainfall in {year_text} with {top_value:,.0f}mm average, {diff:,.0f}mm ({pct:.1f}%) more than {second_state}."
            else:
                answer = "Insufficient data for comparison."

            metadata = {
                "source": "IMD Sub-divisional Monthly Rainfall (data.gov.in)",
                "resource_id": "8e0bd482-4aba-4d99-9cb9-ff124f6f1c2f",
                "filters": {"states": plan.states, "year_start": plan.year_start, "year_end": plan.year_end}
            }

            confidence = self._calculate_confidence(dfs, plan)
            return QueryResult(answer=answer, data=result, metadata=metadata, confidence=confidence)

        else:
            # Unsupported metric
            return QueryResult(
                answer=f"Comparison not supported for metric: {plan.metric}",
                data=pd.DataFrame(),
                metadata={"error": f"Unsupported metric: {plan.metric}"}
            )

    def _execute_extremes(self, plan: QueryPlan) -> QueryResult:
        """Execute extremes query (max/min)."""
        if plan.metric == "production":
            # Handle multi-state extremes comparison (e.g., "highest in State A and lowest in State B")
            if len(plan.states) > 1:
                results = []
                state_details = []
                state_dfs = []  # Track dataframes for confidence calculation
                
                for state in plan.states:
                    # First try with specified year
                    df = self.connector.fetch_production(
                        state=state,
                        crop=plan.crops[0] if plan.crops else None,
                        year=plan.year_start
                    )
                    
                    # If no data with specified year, try without year filter to get most recent
                    if len(df) == 0 and plan.year_start:
                        print(f"   ℹ️  No data for {state} in {plan.year_start}, fetching most recent available...")
                        df = self.connector.fetch_production(
                            state=state,
                            crop=plan.crops[0] if plan.crops else None,
                            year=None  # Get all years
                        )
                        if len(df) > 0:
                            # Find most recent year
                            most_recent_year = df['year'].max()
                            df = df[df['year'] == most_recent_year]
                            print(f"   ✓ Found data for {state} in {most_recent_year}")
                    
                    state_dfs.append(df)  # Store for confidence calculation
                    
                    if len(df) == 0:
                        print(f"   ⚠️  No data available for {state}")
                        continue
                    
                    # Group by district
                    district_totals = df.groupby('district_name')['production_tonne'].sum().reset_index()
                    district_totals = district_totals.sort_values('production_tonne', ascending=False)
                    
                    # Store state data
                    state_details.append({
                        'state': state,
                        'data': district_totals,
                        'top_district': district_totals.iloc[0]['district_name'] if len(district_totals) > 0 else None,
                        'top_value': district_totals.iloc[0]['production_tonne'] if len(district_totals) > 0 else 0,
                        'bottom_district': district_totals.iloc[-1]['district_name'] if len(district_totals) > 0 else None,
                        'bottom_value': district_totals.iloc[-1]['production_tonne'] if len(district_totals) > 0 else 0
                    })
                
                # Handle results even with partial data
                if len(state_details) == 0:
                    answer = f"No data found for {plan.crops[0] if plan.crops else 'crop'} in any of the specified states."
                    combined_result = pd.DataFrame()
                elif len(state_details) == 1:
                    # Partial data - show what we found
                    detail = state_details[0]
                    crop_name = plan.crops[0] if plan.crops else "crop"
                    answer = f"⚠️ Data available only for {detail['state']}:\n"
                    answer += f"   • Highest: {detail['top_district']} ({detail['top_value']:,.0f} tonnes)\n"
                    answer += f"   • Lowest: {detail['bottom_district']} ({detail['bottom_value']:,.0f} tonnes)\n"
                    answer += f"\nNote: No data available for {', '.join([s for s in plan.states if s != detail['state']])}"
                    combined_result = detail['data']
                else:
                    # Generate comparison answer
                    crop_name = plan.crops[0] if plan.crops else "crop"
                    year_text = f" in {plan.year_start}" if plan.year_start else ""
                    
                    # Build answer showing extremes from each state
                    answer_parts = [f"📊 DISTRICT-LEVEL EXTREMES COMPARISON{year_text}:"]
                    
                    for detail in state_details:
                        answer_parts.append(f"\n{detail['state']}:")
                        answer_parts.append(f"   • Highest: {detail['top_district']} ({detail['top_value']:,.0f} tonnes)")
                        answer_parts.append(f"   • Lowest: {detail['bottom_district']} ({detail['bottom_value']:,.0f} tonnes)")
                    
                    # Add direct comparison
                    if len(state_details) == 2:
                        high_state = state_details[0]
                        low_state = state_details[1]
                        
                        # Compare the top districts
                        if high_state['top_value'] > low_state['top_value']:
                            leader = high_state
                            follower = low_state
                        else:
                            leader = low_state
                            follower = high_state
                        
                        diff = leader['top_value'] - follower['top_value']
                        pct = (diff / follower['top_value'] * 100) if follower['top_value'] > 0 else 0
                        
                        answer_parts.append(f"\n🏆 OVERALL: {leader['top_district']} ({leader['state']}) leads with {leader['top_value']:,.0f} tonnes,")
                        answer_parts.append(f"   {diff:,.0f} tonnes ({pct:.1f}%) more than {follower['top_district']} ({follower['state']}).")
                    
                    answer = "\n".join(answer_parts)
                    
                    # Combine top districts from each state for display
                    combined_result = pd.DataFrame([
                        {
                            'state_name': detail['state'],
                            'district_name': detail['top_district'],
                            'production_tonne': detail['top_value'],
                            'rank': 'Highest'
                        }
                        for detail in state_details
                    ] + [
                        {
                            'state_name': detail['state'],
                            'district_name': detail['bottom_district'],
                            'production_tonne': detail['bottom_value'],
                            'rank': 'Lowest'
                        }
                        for detail in state_details
                    ])
                
                metadata = {
                    "source": "District-wise crop production (data.gov.in)",
                    "resource_id": "35be999b-0208-4354-b557-f6ca9a5355de",
                    "filters": {"states": plan.states, "crop": plan.crops[0] if plan.crops else None, "year": plan.year_start}
                }
                
                confidence = self._calculate_confidence(state_dfs, plan)
                return QueryResult(answer=answer, data=combined_result, metadata=metadata, confidence=confidence)
            
            # Single state extremes (original logic)
            else:
                # Fetch production data
                df = self.connector.fetch_production(
                    state=plan.states[0] if plan.states else None,
                    crop=plan.crops[0] if plan.crops else None,
                    year=plan.year_start
                )
                
                # If no data with specified year, try without year filter to get most recent
                if len(df) == 0 and plan.year_start:
                    state_name = plan.states[0] if plan.states else "the state"
                    print(f"   ℹ️  No data for {state_name} in {plan.year_start}, fetching most recent available...")
                    df = self.connector.fetch_production(
                        state=plan.states[0] if plan.states else None,
                        crop=plan.crops[0] if plan.crops else None,
                        year=None  # Get all years
                    )
                    if len(df) > 0:
                        # Find most recent year
                        most_recent_year = df['year'].max()
                        df = df[df['year'] == most_recent_year]
                        print(f"   ✓ Found data in {most_recent_year}")

                # Find district with max production
                result = df.groupby('district_name')['production_tonne'].sum().reset_index()
                result = result.sort_values('production_tonne', ascending=False)

                if len(result) > 0:
                    top_district = result.iloc[0]['district_name']
                    top_value = result.iloc[0]['production_tonne']

                    crop_name = plan.crops[0] if plan.crops else "crop"
                    state_name = plan.states[0] if plan.states else "the state"
                    year_text = f" in {plan.year_start}" if plan.year_start else ""
                    answer = f"{top_district} district in {state_name} has the highest {crop_name} production{year_text} with {top_value:,.0f} tonnes."
                else:
                    answer = "No data found."

                metadata = {
                    "source": "District-wise crop production (data.gov.in)",
                    "resource_id": "35be999b-0208-4354-b557-f6ca9a5355de",
                    "filters": {"state": plan.states[0] if plan.states else None, "crop": plan.crops[0] if plan.crops else None, "year": plan.year_start}
                }

                confidence = self._calculate_confidence([df], plan)
                return QueryResult(answer=answer, data=result.head(10), metadata=metadata, confidence=confidence)
        
        else:
            # Unsupported metric
            return QueryResult(
                answer=f"Extremes analysis not supported for metric: {plan.metric}",
                data=pd.DataFrame(),
                metadata={"error": f"Unsupported metric: {plan.metric}"},
                confidence=0.5
            )

    def _execute_trends(self, plan: QueryPlan) -> QueryResult:
        """Execute trends query."""
        if plan.metric == "rainfall":
            # Fetch rainfall data across years
            df = self.connector.fetch_rainfall_by_state(
                state=plan.states[0],
                year_start=plan.year_start,
                year_end=plan.year_end
            )

            # Sort by year
            result = df.sort_values('year')

            if len(result) > 0:
                start_year = result.iloc[0]['year']
                end_year = result.iloc[-1]['year']
                start_val = result.iloc[0]['rainfall_mm']
                end_val = result.iloc[-1]['rainfall_mm']
                change = end_val - start_val
                pct = (change / start_val * 100) if start_val > 0 else 0
                trend = "increased" if change > 0 else "decreased"

                answer = f"Rainfall in {plan.states[0]} {trend} from {start_val:,.0f}mm ({start_year}) to {end_val:,.0f}mm ({end_year}), a change of {abs(change):,.0f}mm ({abs(pct):.1f}%)."
            else:
                answer = "Insufficient data for trend analysis."

            metadata = {
                "source": "IMD Sub-divisional Monthly Rainfall (data.gov.in)",
                "resource_id": "8e0bd482-4aba-4d99-9cb9-ff124f6f1c2f",
                "filters": {"state": plan.states[0], "year_start": plan.year_start, "year_end": plan.year_end}
            }

            confidence = self._calculate_confidence([df], plan)
            return QueryResult(answer=answer, data=result, metadata=metadata, confidence=confidence)
        
        else:
            # Unsupported metric
            return QueryResult(
                answer=f"Trend analysis not supported for metric: {plan.metric}",
                data=pd.DataFrame(),
                metadata={"error": f"Unsupported metric: {plan.metric}"},
                confidence=0.5
            )

    def _execute_correlation(self, plan: QueryPlan) -> QueryResult:
        """Execute correlation query."""
        print(f"   📊 Fetching production data for {plan.states[0]}...")
        
        # Fetch both production and rainfall data
        prod_df = self.connector.fetch_production(
            state=plan.states[0],
            crop=plan.crops[0] if plan.crops else None
        )
        
        print(f"   📊 Fetching rainfall data for {plan.states[0]}...")
        rain_df = self.connector.fetch_rainfall_by_state(
            state=plan.states[0],
            year_start=plan.year_start,
            year_end=plan.year_end
        )

        if len(prod_df) == 0:
            return QueryResult(
                answer=f"No production data found for {plan.crops[0] if plan.crops else 'crop'} in {plan.states[0]}.",
                data=pd.DataFrame(),
                metadata={"error": "No production data"},
                confidence=0.0
            )
        
        if len(rain_df) == 0:
            return QueryResult(
                answer=f"No rainfall data found for {plan.states[0]}.",
                data=pd.DataFrame(),
                metadata={"error": "No rainfall data"},
                confidence=0.0
            )

        # Aggregate production by year
        prod_by_year = prod_df.groupby('year')['production_tonne'].sum().reset_index()
        prod_by_year.columns = ['year', 'production_tonne']
        
        # Aggregate rainfall by year
        rain_by_year = rain_df.groupby('year')['rainfall_mm'].mean().reset_index()
        rain_by_year.columns = ['year', 'rainfall_mm']
        
        # Debug info
        prod_years = sorted(prod_by_year['year'].unique())
        rain_years = sorted(rain_by_year['year'].unique())
        print(f"   ℹ️  Production years: {prod_years[0]}-{prod_years[-1]} ({len(prod_years)} years)")
        print(f"   ℹ️  Rainfall years: {rain_years[0]}-{rain_years[-1]} ({len(rain_years)} years)")

        # Merge datasets
        merged = pd.merge(prod_by_year, rain_by_year, on='year', how='inner')
        
        print(f"   ✓ Overlapping years: {len(merged)}")
        
        if len(merged) >= 3:
            # Calculate correlation
            corr = merged['production_tonne'].corr(merged['rainfall_mm'])

            if corr > 0.7:
                strength = "strong positive"
            elif corr > 0.3:
                strength = "moderate positive"
            elif corr > -0.3:
                strength = "weak"
            elif corr > -0.7:
                strength = "moderate negative"
            else:
                strength = "strong negative"

            crop_name = plan.crops[0] if plan.crops else "crop"
            year_range = f"{merged['year'].min()}-{merged['year'].max()}"
            
            # Add trend analysis
            prod_trend = "increasing" if merged['production_tonne'].iloc[-1] > merged['production_tonne'].iloc[0] else "decreasing"
            rain_trend = "increasing" if merged['rainfall_mm'].iloc[-1] > merged['rainfall_mm'].iloc[0] else "decreasing"
            
            # Calculate percentage changes
            prod_pct = ((merged['production_tonne'].iloc[-1] - merged['production_tonne'].iloc[0]) / 
                       merged['production_tonne'].iloc[0] * 100) if merged['production_tonne'].iloc[0] > 0 else 0
            rain_pct = ((merged['rainfall_mm'].iloc[-1] - merged['rainfall_mm'].iloc[0]) / 
                       merged['rainfall_mm'].iloc[0] * 100) if merged['rainfall_mm'].iloc[0] > 0 else 0
            
            answer = f"📊 CORRELATION ANALYSIS ({year_range}):\n\n"
            answer += f"Production Trend: {crop_name} production in {plan.states[0]} is {prod_trend} "
            answer += f"({prod_pct:+.1f}% over {len(merged)} years)\n\n"
            answer += f"Climate Trend: Rainfall is {rain_trend} ({rain_pct:+.1f}% over same period)\n\n"
            answer += f"Correlation: {strength} (r={corr:.2f})\n\n"
            
            if abs(corr) > 0.3:
                answer += f"Impact Summary: Rainfall {'positively' if corr > 0 else 'negatively'} correlates with {crop_name} production. "
                if corr > 0:
                    answer += "Higher rainfall is associated with higher production."
                else:
                    answer += "Higher rainfall is associated with lower production (possible flooding/waterlogging)."
            else:
                answer += f"Impact Summary: Weak correlation suggests {crop_name} production is influenced by factors beyond rainfall (e.g., irrigation, soil quality, technology)."
        else:
            # Provide helpful error message
            answer = f"⚠️ Insufficient overlapping data for correlation analysis.\n\n"
            answer += f"Available data:\n"
            answer += f"  • Production: {prod_years[0]}-{prod_years[-1]} ({len(prod_years)} years)\n"
            answer += f"  • Rainfall: {rain_years[0]}-{rain_years[-1]} ({len(rain_years)} years)\n"
            answer += f"  • Overlap: {len(merged)} years (need at least 3)\n\n"
            answer += f"Note: Production data ends at {prod_years[-1]}, while rainfall data is available through {rain_years[-1]}."

        metadata = {
            "sources": [
                {"name": "District-wise crop production", "resource_id": "35be999b-0208-4354-b557-f6ca9a5355de"},
                {"name": "IMD Rainfall", "resource_id": "8e0bd482-4aba-4d99-9cb9-ff124f6f1c2f"}
            ],
            "filters": {"state": plan.states[0], "crop": plan.crops[0] if plan.crops else None},
            "production_years": f"{prod_years[0]}-{prod_years[-1]}" if len(prod_years) > 0 else "none",
            "rainfall_years": f"{rain_years[0]}-{rain_years[-1]}" if len(rain_years) > 0 else "none",
            "overlapping_years": len(merged)
        }

        confidence = self._calculate_confidence([prod_df, rain_df], plan)
        return QueryResult(answer=answer, data=merged, metadata=metadata, confidence=confidence)

    def _execute_multi_part(self, plan: QueryPlan) -> QueryResult:
        """Execute multi-part query combining multiple analyses."""
        
        # Part 1: Rainfall comparison
        rainfall_dfs = []
        for state in plan.states:
            df = self.connector.fetch_rainfall_by_state(
                state=state,
                year_start=plan.year_start,
                year_end=plan.year_end
            )
            rainfall_dfs.append(df)
        
        rainfall_combined = pd.concat(rainfall_dfs, ignore_index=True)
        
        # Calculate average annual rainfall
        rainfall_result = rainfall_combined.groupby('state_name')['rainfall_mm'].mean().reset_index()
        rainfall_result.columns = ['state_name', 'avg_rainfall_mm']
        rainfall_result = rainfall_result.sort_values('avg_rainfall_mm', ascending=False)
        
        # Part 2: Crop production by state
        crop_results = {}
        all_production_dfs = []  # Track all production dataframes for confidence
        
        # Determine if we need specific crops or top N crops
        specific_crops = plan.crops if plan.crops else None
        top_n = 5  # Default for when no specific crops requested
        
        if not specific_crops and plan.sub_queries:
            # Check if query asks for "top N crops"
            for sq in plan.sub_queries:
                if 'top' in sq.lower() and 'crop' in sq.lower():
                    import re
                    match = re.search(r'top\s+(\d+)', sq.lower())
                    if match:
                        top_n = int(match.group(1))
                        break
        
        for state in plan.states:
            # Fetch production data
            state_dfs = []
            
            if specific_crops:
                # Fetch data for each specific crop
                for crop in specific_crops:
                    if plan.year_start and plan.year_end:
                        for year in range(plan.year_start, plan.year_end + 1):
                            year_df = self.connector.fetch_production(
                                state=state,
                                year=year,
                                crop=crop,
                                season=plan.season
                            )
                            state_dfs.append(year_df)
                    else:
                        crop_df = self.connector.fetch_production(
                            state=state,
                            year=None,
                            crop=crop,
                            season=plan.season
                        )
                        state_dfs.append(crop_df)
            else:
                # Fetch all crops to find top N
                if plan.year_start and plan.year_end:
                    for year in range(plan.year_start, plan.year_end + 1):
                        year_df = self.connector.fetch_production(
                            state=state,
                            year=year,
                            crop=None,  # All crops
                            season=plan.season
                        )
                        state_dfs.append(year_df)
                else:
                    # Fetch all available data
                    df = self.connector.fetch_production(
                        state=state,
                        year=None,
                        crop=None,
                        season=plan.season
                    )
                    state_dfs.append(df)
            
            df = pd.concat(state_dfs, ignore_index=True)
            all_production_dfs.extend(state_dfs)  # Track for confidence calculation
            
            # Filter by season if specified (double-check)
            if plan.season and 'season' in df.columns:
                df = df[df['season'].str.strip().str.lower() == plan.season.lower()]
            
            # Group by crop and sum production across all years
            crop_totals = df.groupby('crop')['production_tonne'].sum().reset_index()
            crop_totals = crop_totals.sort_values('production_tonne', ascending=False)
            
            # Get specific crops or top N
            if specific_crops:
                crop_results[state] = crop_totals  # All specified crops
            else:
                crop_results[state] = crop_totals.head(top_n)  # Top N crops
        
        # Generate comprehensive answer
        answer_parts = []
        
        # Rainfall comparison
        year_range = f"{plan.year_start}-{plan.year_end}" if plan.year_start and plan.year_end else "available period"
        answer_parts.append(f"📊 RAINFALL COMPARISON ({year_range}):")
        for idx, row in rainfall_result.iterrows():
            answer_parts.append(f"   • {row['state_name']}: {row['avg_rainfall_mm']:,.0f} mm average annual")
        
        # Crop rankings
        season_text = f" ({plan.season} season)" if plan.season else ""
        if specific_crops:
            # Show specific crops requested
            crops_text = ", ".join(specific_crops)
            answer_parts.append(f"\n🌾 CROP PRODUCTION: {crops_text}{season_text}:")
        else:
            # Show top N crops
            answer_parts.append(f"\n🌾 TOP {top_n} CROPS BY PRODUCTION{season_text}:")
        
        for state, crops_df in crop_results.items():
            answer_parts.append(f"\n   {state}:")
            for idx, row in crops_df.iterrows():
                if specific_crops:
                    # Just show crop name and production (no ranking)
                    answer_parts.append(f"      • {row['crop']}: {row['production_tonne']:,.0f} tonnes")
                else:
                    # Show ranking for top N
                    rank = list(crops_df.index).index(idx) + 1
                    answer_parts.append(f"      {rank}. {row['crop']}: {row['production_tonne']:,.0f} tonnes")
        
        answer = "\n".join(answer_parts)
        
        # Combine data tables
        combined_data = {
            'rainfall': rainfall_result,
            'crops': crop_results
        }
        
        # Create metadata with all sources
        metadata = {
            "sources": [
                {
                    "name": "IMD Sub-divisional Monthly Rainfall",
                    "resource_id": "8e0bd482-4aba-4d99-9cb9-ff124f6f1c2f",
                    "filters": {"states": plan.states, "year_start": plan.year_start, "year_end": plan.year_end}
                },
                {
                    "name": "District-wise crop production",
                    "resource_id": "35be999b-0208-4354-b557-f6ca9a5355de",
                    "filters": {"states": plan.states, "year_start": plan.year_start, "year_end": plan.year_end, "season": plan.season}
                }
            ],
            "sub_queries": plan.sub_queries
        }
        
        # For display, create a simple combined dataframe
        if specific_crops:
            crops_desc = f"Production for {len(specific_crops)} crops: {', '.join(specific_crops)}{season_text}"
        else:
            crops_desc = f"Top {top_n} crops by production{season_text}"
        
        display_df = pd.DataFrame({
            'Analysis': ['Rainfall (avg mm)', 'Crop Production'],
            'Description': [
                f"{len(plan.states)} states compared over {year_range}",
                crops_desc
            ]
        })
        
        # Calculate confidence from all dataframes used
        all_dfs = rainfall_dfs + all_production_dfs
        confidence = self._calculate_confidence(all_dfs, plan)
        
        return QueryResult(answer=answer, data=display_df, metadata=metadata, confidence=confidence)
