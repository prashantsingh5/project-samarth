"""Project Samarth - Advanced Streamlit Web Application"""

import streamlit as st
import os
import base64
from dotenv import load_dotenv
from src import QueryPlanner, QueryExecutor, DataGovInConnector, AnswerGenerator

# Page config
st.set_page_config(
    page_title="Project Samarth - Agricultural Data Q&A",
    page_icon="🌾",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Advanced Custom CSS with glassmorphism and modern design
st.markdown("""
<style>
    /* Import Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap');
    
    /* Global Styles */
    * {
        font-family: 'Inter', sans-serif;
    }
    
    /* Main container with background */
    .main {
        background: linear-gradient(135deg, rgba(15, 23, 42, 0.95) 0%, rgba(30, 41, 59, 0.92) 100%);
        background-attachment: fixed;
    }
    
    /* Background image overlay */
    .stApp {
        background-image: linear-gradient(135deg, rgba(15, 23, 42, 0.93) 0%, rgba(30, 41, 59, 0.90) 100%), 
                          url('https://images.unsplash.com/photo-1625246333195-78d9c38ad449?q=80&w=2000');
        background-size: cover;
        background-position: center;
        background-attachment: fixed;
        background-repeat: no-repeat;
    }
    
    /* Glassmorphism header */
    .hero-section {
        background: linear-gradient(135deg, rgba(34, 197, 94, 0.15) 0%, rgba(16, 185, 129, 0.15) 100%);
        backdrop-filter: blur(20px);
        -webkit-backdrop-filter: blur(20px);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 24px;
        padding: 3rem 2rem;
        text-align: center;
        margin-bottom: 2rem;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        animation: fadeInDown 0.8s ease-out;
    }
    
    .hero-title {
        font-size: 3.5rem;
        font-weight: 700;
        background: linear-gradient(135deg, #10b981 0%, #34d399 50%, #6ee7b7 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.5rem;
        text-shadow: 0 0 40px rgba(16, 185, 129, 0.3);
        letter-spacing: -1px;
    }
    
    .hero-subtitle {
        font-size: 1.3rem;
        color: rgba(255, 255, 255, 0.8);
        font-weight: 300;
        letter-spacing: 0.5px;
    }
    
    /* Chat container */
    .stChatMessage {
        background: rgba(30, 41, 59, 0.6) !important;
        backdrop-filter: blur(10px);
        -webkit-backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 16px !important;
        padding: 1.5rem !important;
        margin-bottom: 1rem;
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.2);
        animation: slideInUp 0.4s ease-out;
    }
    
    /* User message */
    .stChatMessage[data-testid="user-message"] {
        background: linear-gradient(135deg, rgba(16, 185, 129, 0.2) 0%, rgba(5, 150, 105, 0.15) 100%) !important;
        border-left: 3px solid #10b981;
    }
    
    /* Assistant message */
    .stChatMessage[data-testid="assistant-message"] {
        background: rgba(51, 65, 85, 0.5) !important;
        border-left: 3px solid #3b82f6;
    }
    
    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, rgba(15, 23, 42, 0.95) 0%, rgba(30, 41, 59, 0.95) 100%);
        backdrop-filter: blur(10px);
        border-right: 1px solid rgba(255, 255, 255, 0.1);
    }
    
    [data-testid="stSidebar"] .block-container {
        padding-top: 2rem;
    }
    
    /* Example buttons - Neon style */
    .stButton > button {
        width: 100%;
        background: linear-gradient(135deg, rgba(16, 185, 129, 0.15) 0%, rgba(5, 150, 105, 0.1) 100%);
        color: #10b981;
        border: 1px solid rgba(16, 185, 129, 0.3);
        border-radius: 12px;
        padding: 0.8rem 1.2rem;
        font-weight: 500;
        transition: all 0.3s ease;
        backdrop-filter: blur(10px);
        box-shadow: 0 4px 12px rgba(16, 185, 129, 0.1);
    }
    
    .stButton > button:hover {
        background: linear-gradient(135deg, rgba(16, 185, 129, 0.3) 0%, rgba(5, 150, 105, 0.25) 100%);
        border-color: #10b981;
        box-shadow: 0 0 20px rgba(16, 185, 129, 0.4), 0 0 40px rgba(16, 185, 129, 0.2);
        transform: translateY(-2px);
    }
    
    /* Input field */
    .stChatInputContainer {
        background: rgba(30, 41, 59, 0.7) !important;
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 16px;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
    }
    
    /* Confidence badges */
    .confidence-badge {
        display: inline-block;
        padding: 0.5rem 1.2rem;
        border-radius: 20px;
        font-weight: 600;
        font-size: 0.9rem;
        backdrop-filter: blur(10px);
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.2);
    }
    
    .confidence-high {
        background: linear-gradient(135deg, rgba(16, 185, 129, 0.25) 0%, rgba(5, 150, 105, 0.2) 100%);
        color: #6ee7b7;
        border: 1px solid rgba(16, 185, 129, 0.4);
    }
    
    .confidence-medium {
        background: linear-gradient(135deg, rgba(251, 191, 36, 0.25) 0%, rgba(245, 158, 11, 0.2) 100%);
        color: #fcd34d;
        border: 1px solid rgba(251, 191, 36, 0.4);
    }
    
    .confidence-low {
        background: linear-gradient(135deg, rgba(239, 68, 68, 0.25) 0%, rgba(220, 38, 38, 0.2) 100%);
        color: #fca5a5;
        border: 1px solid rgba(239, 68, 68, 0.4);
    }
    
    /* Info cards */
    .info-card {
        background: rgba(30, 41, 59, 0.6);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 16px;
        padding: 1.5rem;
        margin: 1rem 0;
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.2);
    }
    
    .info-card h3 {
        color: #10b981;
        font-size: 1.1rem;
        margin-bottom: 0.8rem;
        font-weight: 600;
    }
    
    .info-card p {
        color: rgba(255, 255, 255, 0.7);
        font-size: 0.95rem;
        line-height: 1.6;
    }
    
    /* Divider */
    hr {
        border: none;
        height: 1px;
        background: linear-gradient(90deg, transparent, rgba(16, 185, 129, 0.3), transparent);
        margin: 2rem 0;
    }
    
    /* Expander */
    .streamlit-expanderHeader {
        background: rgba(30, 41, 59, 0.5);
        border-radius: 12px;
        border: 1px solid rgba(255, 255, 255, 0.08);
        color: rgba(255, 255, 255, 0.9);
    }
    
    /* Animations */
    @keyframes fadeInDown {
        from {
            opacity: 0;
            transform: translateY(-30px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }
    
    @keyframes slideInUp {
        from {
            opacity: 0;
            transform: translateY(20px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }
    
    /* Scrollbar - Lighter Green for Visibility */
    ::-webkit-scrollbar {
        width: 10px;
        height: 10px;
    }
    
    ::-webkit-scrollbar-track {
        background: rgba(30, 41, 59, 0.5);
    }
    
    ::-webkit-scrollbar-thumb {
        background: linear-gradient(135deg, #4ade80 0%, #22c55e 100%);
        border-radius: 5px;
    }
    
    ::-webkit-scrollbar-thumb:hover {
        background: linear-gradient(135deg, #86efac 0%, #4ade80 100%);
    }
    
    /* Text colors */
    .stMarkdown, p, li {
        color: rgba(255, 255, 255, 0.85);
    }
    
    h1, h2, h3, h4, h5, h6 {
        color: rgba(255, 255, 255, 0.95);
    }
    
    /* Status messages */
    .stAlert {
        background: rgba(30, 41, 59, 0.7);
        backdrop-filter: blur(10px);
        border-radius: 12px;
        border: 1px solid rgba(255, 255, 255, 0.1);
    }
    
    /* Spinner */
    .stSpinner > div {
        border-top-color: #10b981 !important;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def initialize_system():
    """Initialize the Q&A system components."""
    load_dotenv()
    
    data_gov_key = os.getenv('DATA_GOV_IN_API_KEY')
    gemini_key = os.getenv('GEMINI_API_KEY')
    
    if not data_gov_key or not gemini_key:
        st.error("❌ API keys not found! Please set DATA_GOV_IN_API_KEY and GEMINI_API_KEY in Hugging Face Spaces secrets.")
        st.stop()
    
    try:
        connector = DataGovInConnector(api_key=data_gov_key)
        planner = QueryPlanner(api_key=gemini_key)
        executor = QueryExecutor(connector)
        answer_gen = AnswerGenerator(api_key=gemini_key)
        return planner, executor, answer_gen
    except Exception as e:
        st.error(f"❌ Failed to initialize system: {e}")
        st.stop()


def get_confidence_badge(confidence: float) -> str:
    """Generate modern confidence badge HTML."""
    pct = confidence * 100
    if pct >= 90:
        return f'<span class="confidence-badge confidence-high">✓ HIGH CONFIDENCE {pct:.0f}%</span>'
    elif pct >= 70:
        return f'<span class="confidence-badge confidence-medium">◐ MEDIUM CONFIDENCE {pct:.0f}%</span>'
    else:
        return f'<span class="confidence-badge confidence-low">! LOW CONFIDENCE {pct:.0f}%</span>'


def main():
    # Hero Section with glassmorphism
    st.markdown("""
    <div class="hero-section">
        <div class="hero-title">🌾 PROJECT SAMARTH</div>
        <div class="hero-subtitle">AI-Powered Agricultural Intelligence Platform</div>
    </div>
    """, unsafe_allow_html=True)
    
    # Initialize system
    planner, executor, answer_gen = initialize_system()
    
    # Sidebar with modern cards
    with st.sidebar:
        st.markdown("""
        <div class="info-card">
            <h3>🤖 About Platform</h3>
            <p>Advanced AI-powered analytics system combining live agricultural data with intelligent insights.</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("### ⚡ Quick Examples")
        examples = [
            ("🌾 Production Comparison", "Which state had more rice production in 2015 - Karnataka or Tamil Nadu?"),
            ("🌧️ Climate Analysis", "Compare rainfall in Karnataka vs Tamil Nadu for last 5 years"),
            ("📊 District Insights", "Which district in Karnataka has the highest maize production?"),
            ("🔗 Correlation Study", "Is there a correlation between rainfall and rice production in Tamil Nadu?"),
            ("📋 Policy Analysis", "What are three arguments to promote Bajra over Rice in Rajasthan?")
        ]
        
        for i, (label, question) in enumerate(examples, 1):
            if st.button(label, key=f"ex_{i}", use_container_width=True):
                st.session_state.example_query = question
        
        st.divider()
        
        st.markdown("""
        <div class="info-card">
            <h3>📡 Data Sources</h3>
            <p>
            • District Crop Production<br>
            • IMD Rainfall (1901-2017)<br>
            • Agricultural Market Prices<br>
            • State & District Coverage
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
        <div class="info-card">
            <h3>✨ Features</h3>
            <p>
            • Natural Language Processing<br>
            • Real-time Data Integration<br>
            • AI-Enhanced Insights<br>
            • Confidence Scoring<br>
            • Source Citations
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        if st.button("🗑️ Clear Chat History", key="clear_btn", use_container_width=True):
            st.session_state.messages = []
            st.rerun()
    
    # Initialize chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []
    
    # Welcome message with modern styling
    if len(st.session_state.messages) == 0:
        st.markdown("""
        <div class="info-card" style="text-align: center; padding: 2rem;">
            <h2 style="color: #10b981; margin-bottom: 1rem;">👋 Welcome to the Future of Agricultural Intelligence</h2>
            <p style="font-size: 1.1rem; line-height: 1.8;">
                Ask complex questions in natural language and receive AI-powered insights backed by real data.
            </p>
            <p style="color: rgba(255, 255, 255, 0.6); margin-top: 1rem;">
                💬 Type your question below or select a quick example from the sidebar →
            </p>
        </div>
        """, unsafe_allow_html=True)
    
    # Display chat history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"], unsafe_allow_html=True)
            if "metadata" in message:
                with st.expander("📊 View Details"):
                    st.json(message["metadata"])
    
    # Handle example query
    if "example_query" in st.session_state:
        query = st.session_state.example_query
        del st.session_state.example_query
    else:
        query = st.chat_input("Ask me anything about Indian agriculture...")
    
    # Process query
    if query:
        # Add user message
        st.session_state.messages.append({"role": "user", "content": query})
        with st.chat_message("user"):
            st.markdown(query)
        
        # Process with assistant
        with st.chat_message("assistant"):
            try:
                # Simple, clean progress indicator
                with st.spinner("Analyzing..."):
                    plan = planner.parse_question(query)
                    result = executor.execute(plan)
                    data_summary = answer_gen.extract_data_summary(plan, result.data, result.metadata)
                    enhanced_answer = answer_gen.generate_answer(
                        original_question=query,
                        query_plan=plan,
                        raw_answer=result.answer,
                        data_summary=data_summary,
                        metadata=result.metadata
                    )
                
                # Display answer
                st.markdown(enhanced_answer)
                
                # Display confidence with modern badge
                st.markdown("<br>", unsafe_allow_html=True)
                st.markdown(
                    f'<div style="text-align: center; margin: 1.5rem 0;">{get_confidence_badge(result.confidence)}</div>',
                    unsafe_allow_html=True
                )
                
                # Create response message
                response_content = f"{enhanced_answer}\n\n---\n**Confidence:** {get_confidence_badge(result.confidence)}"
                
                # Add to history
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": response_content,
                    "metadata": {
                        "intent": plan.intent,
                        "states": plan.states,
                        "crops": plan.crops,
                        "years": f"{plan.year_start}-{plan.year_end}",
                        "confidence": f"{result.confidence*100:.0f}%"
                    }
                })
                
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": f"❌ Sorry, I encountered an error: {str(e)}\n\nPlease try rephrasing your question."
                })


if __name__ == "__main__":
    main()
