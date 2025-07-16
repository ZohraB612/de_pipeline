import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta
import os
from typing import Dict, List, Any
import json
import warnings
import re
warnings.filterwarnings('ignore')

# Configuration
API_BASE_URL = os.environ.get('API_BASE_URL', 'http://localhost:5001')

def make_api_request(endpoint: str, method: str = 'GET', **kwargs) -> Dict[str, Any]:
    """Make API request with error handling"""
    try:
        url = f"{API_BASE_URL}{endpoint}"
        response = requests.request(method, url, **kwargs)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"API request failed: {str(e)}")
        return {}

def upload_file_to_api(file) -> Dict[str, Any]:
    """Upload file to API endpoint"""
    try:
        url = f"{API_BASE_URL}/nhs-data/upload"
        files = {"file": file}
        response = requests.post(url, files=files)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"File upload failed: {str(e)}")
        return {}

def trigger_pipeline(url: str) -> Dict[str, Any]:
    """Trigger NHS data processing pipeline"""
    try:
        api_url = f"{API_BASE_URL}/nhs-data/trigger-pipeline"
        
        # --- DEBUGGING LINE ---
        # This will show the exact URL being called in the Streamlit UI
        st.info(f"--> Streamlit is sending request to URL: {api_url}")
        
        # First try to check if the API is reachable
        try:
            health_response = requests.get(f"{API_BASE_URL}/health", timeout=5)
            if health_response.status_code == 200:
                st.success("✅ API health check passed")
            else:
                st.warning(f"⚠️ API health check returned: {health_response.status_code}")
        except Exception as health_error:
            st.error(f"❌ API health check failed: {str(health_error)}")
            return {}
        
        response = requests.post(api_url, json={"url": url})
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Pipeline trigger failed: {str(e)}")
        st.info("💡 Try restarting the API service: `docker-compose restart api`")
        return {}

def get_flow_status(flow_run_id: str) -> Dict[str, Any]:
    """Get status of a specific flow run"""
    return make_api_request(f"/nhs-data/status/{flow_run_id}")

def extract_quarter_year_from_filename(file_path: str) -> tuple:
    """Extract quarter and year from filename patterns."""
    filename = os.path.basename(file_path)
    
    # Remove MinIO timestamp prefix if present
    cleaned_filename = re.sub(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+_', '', filename)
    
    # Try to extract quarter from patterns like "Q4", "Quarter 4", etc.
    quarter_match = re.search(r'Quarter (\d)|Q(\d)', cleaned_filename, re.IGNORECASE)
    
    # Try to extract year from patterns like "2024-25", "2023-24", or "2015-16"
    year_match = re.search(r'(\d{4})-(\d{2})', cleaned_filename)
    if not year_match:
        year_match = re.search(r'(\d{4})', cleaned_filename)
    
    quarter = f"Q{quarter_match.group(1) or quarter_match.group(2)}" if quarter_match else "Q1"
    
    year = int(year_match.group(1)) if year_match else datetime.now().year
    
    return quarter, year

def main():
    st.set_page_config(
        page_title="NHS Beds Dashboard",
        page_icon=":bar_chart:",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # Custom CSS for NHS-style theming with dark mode support
    st.markdown("""
    <style>
        .main-header {
            background-color: #005EB8;
            color: white;
            padding: 1rem;
            margin: -1rem -1rem 2rem -1rem;
            border-radius: 0;
        }
        .metric-card {
            background-color: rgba(248, 249, 250, 0.1);
            padding: 1rem;
            border-radius: 8px;
            border-left: 4px solid #005EB8;
            margin: 0.5rem 0;
            border: 1px solid rgba(255, 255, 255, 0.1);
        }
        .filter-section {
            background-color: rgba(248, 249, 250, 0.05);
            padding: 1rem;
            border-radius: 8px;
            margin-bottom: 1rem;
            border: 1px solid rgba(255, 255, 255, 0.1);
        }
        .kpi-container {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 1rem;
            border-radius: 12px;
            text-align: center;
            margin: 0.5rem 0;
            border: 1px solid rgba(255, 255, 255, 0.2);
        }
        .kpi-title {
            font-size: 0.9rem;
            font-weight: 600;
            margin-bottom: 0.5rem;
            color: white;
        }
        .kpi-value {
            font-size: 2.5rem;
            font-weight: 700;
            line-height: 1;
            color: white;
        }
        .kpi-subtitle {
            font-size: 0.8rem;
            opacity: 0.9;
            margin-top: 0.3rem;
            color: white;
        }
        /* Dark mode text visibility */
        .stMarkdown, .stText {
            color: inherit !important;
        }
        /* Ensure chart backgrounds work in dark mode */
        .js-plotly-plot .plotly .main-svg {
            background: transparent !important;
        }
    </style>
    """, unsafe_allow_html=True)

    # Header
    st.markdown("""
    <div class="main-header">
        <h1>NHS Beds Dashboard | Bed Availability & Occupancy Analytics</h1>
        <p>Real-time monitoring of bed availability and occupancy across NHS Trusts and Regions</p>
    </div>
    """, unsafe_allow_html=True)

    # Sidebar filters
    st.sidebar.markdown("### Dashboard Sections")

    with st.sidebar:
        st.markdown('<div class="filter-section">', unsafe_allow_html=True)
        
        # Navigation
        page = st.radio(
            "Select Section",
            [
                "Executive Dashboard",
                "Data Upload",
                "Pipeline Status"
            ],
            index=0
        )
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Regional filters for Executive Dashboard
        if page == "Executive Dashboard":
            st.markdown('<div class="filter-section">', unsafe_allow_html=True)
            st.markdown("#### Dashboard Filters")
            
            # Period filtering - NHS Financial Year format (2024-25, 2023-24, etc.)
            periods_data = load_available_periods()
            available_years = periods_data.get("available_years", [])
            periods_by_year = periods_data.get("periods_by_year", {})
            
            # Convert single years to NHS financial year format
            
            # Create financial year options
            financial_years = []
            financial_year_mapping = {}  # Maps display format to actual year
            
            for year in available_years:
                financial_year = format_financial_year(year)
                financial_years.append(financial_year)
                financial_year_mapping[financial_year] = year
            
            # Year filter with NHS financial year format
            year_options = ["All Years"] + sorted(financial_years, reverse=True)
            selected_financial_year = st.selectbox("Select Financial Year", year_options, index=0)
            
            # Quarter filter (based on selected year)
            if selected_financial_year == "All Years":
                quarter_options = ["All Quarters"]
                selected_quarter = "All Quarters"
            else:
                # Get the actual year from the financial year
                actual_year = financial_year_mapping.get(selected_financial_year)
                if actual_year:
                    available_quarters = periods_by_year.get(str(actual_year), [])
                    # Ensure quarters are in Q1, Q2, Q3, Q4 format
                    standard_quarters = ["Q1", "Q2", "Q3", "Q4"]
                    available_quarters = [q for q in standard_quarters if q in available_quarters]
                    quarter_options = ["All Quarters"] + available_quarters
                    selected_quarter = st.selectbox("Select Quarter", quarter_options, index=0)
                else:
                    quarter_options = ["All Quarters"]
                    selected_quarter = "All Quarters"
            
            # Convert filters to API parameters
            year_param = None if selected_financial_year == "All Years" else financial_year_mapping.get(selected_financial_year)
            quarter_param = None if selected_quarter == "All Quarters" else selected_quarter
            
            # Regional filtering
            if 'uploaded_file_path' in st.session_state:
                # Get regions from uploaded file
                file_speciality_df = load_speciality_data_from_file(st.session_state['uploaded_file_path'])
                if file_speciality_df is not None and 'region_code' in file_speciality_df.columns:
                    unique_regions = file_speciality_df['region_code'].dropna().unique()
                    available_regions = ["All Organizations"] + sorted([r for r in unique_regions if r.strip()])
                else:
                    available_regions = ["All Organizations"]
            else:
                available_regions = get_available_regions()
            
            region_filter = st.selectbox(
                "Select Region",
                available_regions,
                index=0
            )
            
            # Check if filters have changed and clear cache if needed
            filter_key = f"{year_param}_{quarter_param}_{region_filter}"
            if 'last_filter_key' not in st.session_state or st.session_state.last_filter_key != filter_key:
                st.cache_data.clear()
                st.session_state.last_filter_key = filter_key
            
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            region_filter = "All Organizations"
            year_param = None
            quarter_param = None
            
            # Clear cache for non-Executive Dashboard to ensure fresh data
            if 'last_filter_key' in st.session_state:
                st.cache_data.clear()
                del st.session_state.last_filter_key

    # Route to appropriate page
    if page == "Executive Dashboard":
        show_executive_dashboard(region_filter, year_param, quarter_param)
    elif page == "Data Upload":
        show_upload_page()
    elif page == "Pipeline Status":
        show_pipeline_status()
    

@st.cache_data(ttl=60)  # Cache for 60 seconds
def load_and_process_data(year=None, quarter=None):
    """Load and process NHS bed occupancy data from API."""
    with st.spinner("Loading data..."):
        # Build query parameters
        params = {}
        if year is not None:
            params['year'] = year
        if quarter is not None:
            params['quarter'] = quarter
        
        # Make API request with parameters
        if params:
            query_string = "&".join([f"{k}={v}" for k, v in params.items()])
            endpoint = f"/nhs-data/occupancy?{query_string}"
        else:
            endpoint = "/nhs-data/occupancy"
            
        data = make_api_request(endpoint)
    
    if not data or not isinstance(data, list):
        return None
    
    df = pd.DataFrame(data)
    if df.empty:
        return None
    
    # Add derived columns
    if 'beds_occupied' in df.columns and 'beds_available' in df.columns:
        df['total_beds'] = df['beds_occupied'] + df['beds_available']
        df['spare_capacity'] = df['beds_available']
    elif 'occupancy_rate' in df.columns and 'beds_occupied' in df.columns:
        valid_mask = df['occupancy_rate'] > 0
        df.loc[valid_mask, 'total_beds'] = df.loc[valid_mask, 'beds_occupied'] / (df.loc[valid_mask, 'occupancy_rate'] / 100)
        df.loc[valid_mask, 'spare_capacity'] = df.loc[valid_mask, 'total_beds'] - df.loc[valid_mask, 'beds_occupied']
    
    # Add risk levels
    if 'occupancy_rate' in df.columns:
        df['risk_level'] = pd.cut(
            df['occupancy_rate'], 
            bins=[-np.inf, 90, 95, np.inf], 
            labels=["🟢 Low", "🟡 Medium", "🔴 High"]
        )
    
    return df

@st.cache_data
def load_speciality_data_from_file(file_path):
    """Load speciality data directly from Excel file."""
    if not file_path:
        return None
        
    try:
        # Try different possible sheet names for speciality data
        possible_sheets = [
            'Occupied by Speciality', 
            'Occupied by Specialty',
            'By Speciality',
            'By Specialty',
            'Speciality',
            'Specialty'
        ]
        
        df = None
        for sheet_name in possible_sheets:
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name, header=11)
                st.success(f"Found speciality data in sheet: '{sheet_name}'")
                break
            except ValueError:
                continue
        
        if df is None:
            # Try to find any sheet with 'special' in the name
            xl_file = pd.ExcelFile(file_path)
            speciality_sheets = [sheet for sheet in xl_file.sheet_names if 'special' in sheet.lower()]
            if speciality_sheets:
                df = pd.read_excel(file_path, sheet_name=speciality_sheets[0], header=11)
                st.success(f"Found speciality data in sheet: '{speciality_sheets[0]}'")
            else:
                st.warning("No 'Occupied by Speciality' sheet found in the Excel file")
                return None
        
        if df.empty:
            return None

        # Process the speciality data similar to the pipeline
        expected_cols = ['Year', 'Period End', 'Region Code', 'Org Code', 'Org Name']
        
        # Check if we have the basic organizational columns
        if not all(col in df.columns for col in ['Org Code', 'Org Name']):
            st.error("Missing required columns 'Org Code' and 'Org Name' in speciality sheet")
            return None

        processed_records = []
        
        # Get all speciality columns (everything after the basic org columns)
        speciality_columns = [col for col in df.columns if col not in expected_cols and col.strip()]
        
        for _, row in df.iterrows():
            org_code = row['Org Code']
            org_name = row['Org Name']
            region_code = row.get('Region Code', '')
            
            # Skip if missing org info
            if pd.isna(org_code) or pd.isna(org_name):
                continue
                
            # Skip the England total row
            if str(org_code).strip().lower() in ['england', 'total']:
                continue
            
            # Process each speciality column
            for speciality_col in speciality_columns:
                occupied_beds = pd.to_numeric(row[speciality_col], errors='coerce')
                
                # Only include if we have valid occupied bed data
                if not pd.isna(occupied_beds) and occupied_beds > 0:
                    # Extract speciality code and name from column header
                    speciality_parts = speciality_col.split(' ', 1)
                    if len(speciality_parts) >= 2:
                        speciality_code = speciality_parts[0]
                        speciality_name = speciality_parts[1]
                    else:
                        speciality_code = speciality_col
                        speciality_name = speciality_col
                    
                    processed_records.append({
                        'organisation_code': str(org_code).strip(),
                        'organisation_name': str(org_name).strip(),
                        'region_code': str(region_code).strip() if not pd.isna(region_code) else '',
                        'speciality': speciality_name.strip(),
                        'speciality_code': speciality_code.strip(),
                        'beds_occupied': int(occupied_beds),
                    })
        
        if processed_records:
            result_df = pd.DataFrame(processed_records)
            # Extract and add year/quarter
            quarter, year = extract_quarter_year_from_filename(file_path)
            result_df['quarter'] = quarter
            result_df['year'] = year
            st.success(f"Processed {len(result_df)} speciality records from Excel file for {quarter} {year}")
            return result_df
        else:
            st.warning("No valid speciality data found in the Excel file")
            return None
            
    except Exception as e:
        st.error(f"Failed to load speciality data from Excel file: {str(e)}")
        return None

@st.cache_data(ttl=60)  # Cache for 60 seconds
def load_speciality_data(year=None, quarter=None):
    """Load speciality bed occupancy data from API or file."""
    try:
        with st.spinner("Loading speciality data..."):
            # Build query parameters
            params = {}
            if year is not None:
                params['year'] = year
            if quarter is not None:
                params['quarter'] = quarter
            
            # Make API request with parameters
            if params:
                query_string = "&".join([f"{k}={v}" for k, v in params.items()])
                endpoint = f"/nhs-data/occupancy-by-speciality?{query_string}"
            else:
                endpoint = "/nhs-data/occupancy-by-speciality"
                
            data = make_api_request(endpoint)
        
        if not data or not isinstance(data, list):
            return None
        
        df = pd.DataFrame(data)
        return df if not df.empty else None
    except Exception as e:
        st.caption(f"API unavailable: {str(e)}")
        return None

@st.cache_data(ttl=60)  # Cache for 60 seconds
def load_region_mapping(year=None, quarter=None):
    """Load region mapping from speciality data which includes Region Code."""
    try:
        speciality_data = load_speciality_data(year, quarter)
        if not speciality_data:
            return {}
        
        # Create mapping from org code to region
        region_mapping = {}
        for record in speciality_data:
            org_code = record.get('organisation_code')
            region_code = record.get('region_code', '')
            if org_code and region_code:
                region_mapping[org_code] = region_code
        
        return region_mapping
    except Exception:
        return {}

@st.cache_data(ttl=60)  # Cache for 60 seconds
def load_available_periods():
    """Load available years and quarters from API."""
    try:
        data = make_api_request("/nhs-data/available-periods")
        if not data:
            return {"available_years": [], "periods_by_year": {}}
        return data
    except Exception:
        return {"available_years": [], "periods_by_year": {}}

@st.cache_data(ttl=60)  # Cache for 60 seconds
def get_available_regions():
    """Get list of available regions from the actual data."""
    try:
        speciality_data = make_api_request("/nhs-data/occupancy-by-speciality")
        if not speciality_data:
            return ["All Organizations"]
        
        # Extract unique regions from the data
        regions = set()
        for record in speciality_data:
            region_code = record.get('region_code', '')
            if region_code and region_code.strip():
                regions.add(region_code.strip())
        
        region_list = ["All Organizations"] + sorted(list(regions))
        return region_list
    except Exception:
        return ["All Organizations"]

def format_financial_year(year):
    """Convert year (2024) to NHS financial year format (2024-25)"""
    if isinstance(year, (int, float)):
        next_year = int(year) + 1
        return f"{int(year)}-{str(next_year)[-2:]}"
    return str(year)

def show_executive_dashboard(region_filter="All Organizations", year=None, quarter=None):
    col1, col2 = st.columns([3, 1])
    with col1:
        # Build title with period info
        period_info = []
        if year is not None:
            period_info.append(f"Year: {format_financial_year(year)}")
        if quarter is not None:
            period_info.append(f"Quarter: {quarter}")
        
        period_text = f" ({', '.join(period_info)})" if period_info else ""
        st.markdown(f"### Key Performance Indicators - {region_filter}{period_text}")
    with col2:
        if st.button("🔄 Refresh Data", help="Clear cache and reload data", key="refresh_button"):
            st.cache_data.clear()
            st.rerun()
    
    df = load_and_process_data(year, quarter)
    if df is None:
        st.warning("No occupancy data available for the selected period. Please upload data or trigger the pipeline first.")
        return
    
    # Add region mapping to dataframe using real data
    region_mapping = load_region_mapping(year, quarter)
    if 'organisation_code' in df.columns and region_mapping:
        df['region_code'] = df['organisation_code'].map(region_mapping)
    
    # Remove duplicates to ensure each organization appears only once
    df = df.drop_duplicates(subset=['organisation_code'], keep='last')
    
    # Filter data based on region selection
    if region_filter == "All Organizations":
        filtered_df = df
    else:
        if 'region_code' in df.columns:
            filtered_df = df[df['region_code'] == region_filter]
            if len(filtered_df) == 0:
                st.warning(f"No organizations found for {region_filter} region. Showing all organizations.")
                filtered_df = df
        else:
            filtered_df = df
    
    # Calculate actual metrics from the real data
    total_beds = filtered_df['total_beds'].sum() if 'total_beds' in filtered_df.columns else 0
    total_occupied = filtered_df['beds_occupied'].sum() if 'beds_occupied' in filtered_df.columns else 0
    total_available = filtered_df['beds_available'].sum() if 'beds_available' in filtered_df.columns else 0
    avg_occupancy_rate = filtered_df['occupancy_rate'].mean() if 'occupancy_rate' in filtered_df.columns else 0
    # Calculate high risk count based on data (top quartile)
    if len(filtered_df) > 0 and 'occupancy_rate' in filtered_df.columns:
        high_risk_threshold = filtered_df['occupancy_rate'].quantile(0.75)
        high_risk_count = len(filtered_df[filtered_df['occupancy_rate'] > high_risk_threshold])
    else:
        high_risk_count = 0
        high_risk_threshold = 0
    spare_capacity = total_available
    
    # KPI Cards Row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"""
        <div class="kpi-container">
            <div class="kpi-title">Average Occupancy Rate</div>
            <div class="kpi-value">{avg_occupancy_rate:.1f}%</div>
            <div class="kpi-subtitle">{len(filtered_df)} Organizations</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="kpi-container" style="background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);">
            <div class="kpi-title">Total Beds</div>
            <div class="kpi-value">{total_beds:,.0f}</div>
            <div class="kpi-subtitle">Available + Occupied</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="kpi-container" style="background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);">
            <div class="kpi-title">High Risk Organizations</div>
            <div class="kpi-value">{high_risk_count}</div>
            <div class="kpi-subtitle">>Top 25%</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown(f"""
        <div class="kpi-container" style="background: linear-gradient(135deg, #fa709a 0%, #fee140 100%);">
            <div class="kpi-title">Available Beds</div>
            <div class="kpi-value">{spare_capacity:,.0f}</div>
            <div class="kpi-subtitle">Current Capacity</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Charts Row 1
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### NHS Bed Occupancy Rate Over Time")
        
        if len(filtered_df) > 0:
            # Load actual quarterly trend data from database
            @st.cache_data(ttl=60)
            def load_quarterly_trend_data(selected_year, selected_quarter, region_filter_key):
                # Get data, filtered by year if a year is selected
                all_data = load_and_process_data(year=selected_year) 
                if all_data is None or len(all_data) == 0:
                    return None
                
                # Check if we have year and quarter columns
                if 'year' not in all_data.columns or 'quarter' not in all_data.columns:
                    return None
                
                # Group by year and quarter to get average occupancy rate per quarter
                all_data['year'] = pd.to_numeric(all_data['year'])
                quarterly_data = all_data.groupby(['year', 'quarter'])['occupancy_rate'].mean().reset_index()
                
                # Create a proper date column for each quarter
                def quarter_to_date(row):
                    year_val = int(row['year'])
                    quarter = row['quarter']
                    if quarter == 'Q1':
                        return pd.Timestamp(year_val, 3, 31)  # End of Q1
                    elif quarter == 'Q2':
                        return pd.Timestamp(year_val, 6, 30)  # End of Q2
                    elif quarter == 'Q3':
                        return pd.Timestamp(year_val, 9, 30)  # End of Q3
                    elif quarter == 'Q4':
                        return pd.Timestamp(year_val, 12, 31) # End of Q4
                    else:
                        return pd.Timestamp(year_val, 6, 30) # Default to Q2
                
                quarterly_data['Date'] = quarterly_data.apply(quarter_to_date, axis=1)
                quarterly_data['period_label'] = quarterly_data['quarter'] + ' ' + quarterly_data['year'].astype(str)
                
                # Sort by date
                quarterly_data = quarterly_data.sort_values('Date')
                
                # If a specific quarter is applied, highlight it
                if selected_quarter is not None and selected_year is not None:
                    quarterly_data['is_selected'] = (quarterly_data['year'] == selected_year) & (quarterly_data['quarter'] == selected_quarter)
                
                return quarterly_data
            
            trend_data = load_quarterly_trend_data(year, quarter, region_filter)
            
            if trend_data is not None and len(trend_data) > 0:
                # Create the base line chart
                fig_trend = px.line(
                    trend_data,
                    x='Date',
                    y='occupancy_rate',
                    title=f"NHS Bed Occupancy Rate by Quarter - {region_filter}" + (f" ({format_financial_year(year) if year else 'All Years'})" if year else "") + (f" {quarter if quarter else ''}"),
                    labels={'occupancy_rate': 'Occupancy Rate (%)', 'Date': 'Date'},
                    markers=True,
                    hover_data=['period_label']
                )
                
                # If we have selected periods, highlight them
                if 'is_selected' in trend_data.columns:
                    selected_data = trend_data[trend_data['is_selected'] == True]
                    if len(selected_data) > 0:
                        fig_trend.add_scatter(
                            x=selected_data['Date'],
                            y=selected_data['occupancy_rate'],
                            mode='markers',
                            marker=dict(size=12, color='red', symbol='star'),
                            name='Selected Period',
                            hovertemplate='<b>%{customdata}</b><br>Occupancy Rate: %{y:.1f}%<extra></extra>',
                            customdata=selected_data['period_label']
                        )
                
                # Add target lines
                fig_trend.add_hline(y=90, line_dash="dash", line_color="orange", annotation_text="Target: 90%")
                fig_trend.add_hline(y=95, line_dash="dash", line_color="red", annotation_text="High Risk: 95%")

                # --- DYNAMIC Y-AXIS SCALING ---
                min_rate = trend_data['occupancy_rate'].min()
                max_rate = trend_data['occupancy_rate'].max()
                padding = (max_rate - min_rate) * 0.1 # Add 10% padding
                y_axis_range = [max(0, min_rate - padding), min(100, max_rate + padding)]

            else:
                # Fallback for no data
                fig_trend = go.Figure()
                fig_trend.add_annotation(text="No data available", xref="paper", yref="paper", x=0.5, y=0.5)
                y_axis_range = [80, 100] # Default range

        else:
            # Fallback for no data
            fig_trend = go.Figure()
            fig_trend.add_annotation(text="No data available", xref="paper", yref="paper", x=0.5, y=0.5)
            y_axis_range = [80, 100] # Default range
        
        # Add reference line at current average for the line chart
        if len(filtered_df) > 0 and 'occupancy_rate' in filtered_df.columns:
            current_avg = filtered_df['occupancy_rate'].mean()
            fig_trend.add_hline(y=current_avg, line_dash="dash", line_color="white", annotation_text=f"Current Period Avg: {current_avg:.1f}%")
        
        fig_trend.update_traces(line_color='#005EB8', line_width=3)
        fig_trend.update_layout(
            height=400,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font_color='white',
            yaxis_range=y_axis_range  # Apply the dynamic range
        )
        st.plotly_chart(fig_trend, use_container_width=True, key=f"trend_chart_{region_filter}_{year or 'all'}_{quarter or 'all'}")
    
    with col2:
        st.markdown("### Key Metrics")
        
        if len(filtered_df) > 0 and 'occupancy_rate' in filtered_df.columns:
            # Calculate key metrics
            avg_occupancy = filtered_df['occupancy_rate'].mean()
            capacity_utilization = (filtered_df['beds_occupied'].sum() / filtered_df['total_beds'].sum() * 100) if 'total_beds' in filtered_df.columns else avg_occupancy
            
            # Calculate risk level (percentage of organizations above 90% occupancy)
            high_risk_orgs = len(filtered_df[filtered_df['occupancy_rate'] > 90])
            risk_percentage = (high_risk_orgs / len(filtered_df)) * 100
            
            # Create gauge charts
            fig_gauges = make_subplots(
                rows=3, cols=1,
                subplot_titles=("Average Occupancy", "Capacity Utilization", "Risk Level"),
                specs=[[{"type": "indicator"}], [{"type": "indicator"}], [{"type": "indicator"}]],
                vertical_spacing=0.15
            )
            
            # Average Occupancy Gauge
            fig_gauges.add_trace(
                go.Indicator(
                    mode="gauge+number",
                    value=avg_occupancy,
                    domain={'x': [0, 1], 'y': [0, 1]},
                    title={'text': ""},
                    gauge={
                        'axis': {'range': [None, 100]},
                        'bar': {'color': "#005EB8"},
                        'steps': [
                            {'range': [0, 70], 'color': "#E8F5E8"},
                            {'range': [70, 85], 'color': "#FFF3CD"},
                            {'range': [85, 100], 'color': "#F8D7DA"}
                        ],
                        'threshold': {
                            'line': {'color': "red", 'width': 4},
                            'thickness': 0.75,
                            'value': 90
                        }
                    },
                    number={'suffix': "%", 'font': {'size': 16}}
                ),
                row=1, col=1
            )
            
            # Capacity Utilization Gauge
            fig_gauges.add_trace(
                go.Indicator(
                    mode="gauge+number",
                    value=capacity_utilization,
                    domain={'x': [0, 1], 'y': [0, 1]},
                    title={'text': ""},
                    gauge={
                        'axis': {'range': [None, 100]},
                        'bar': {'color': "#28A745"},
                        'steps': [
                            {'range': [0, 70], 'color': "#E8F5E8"},
                            {'range': [70, 85], 'color': "#FFF3CD"},
                            {'range': [85, 100], 'color': "#F8D7DA"}
                        ],
                        'threshold': {
                            'line': {'color': "red", 'width': 4},
                            'thickness': 0.75,
                            'value': 90
                        }
                    },
                    number={'suffix': "%", 'font': {'size': 16}}
                ),
                row=2, col=1
            )
            
            # Risk Level Gauge
            fig_gauges.add_trace(
                go.Indicator(
                    mode="gauge+number",
                    value=risk_percentage,
                    domain={'x': [0, 1], 'y': [0, 1]},
                    title={'text': ""},
                    gauge={
                        'axis': {'range': [None, 100]},
                        'bar': {'color': "#DC3545"},
                        'steps': [
                            {'range': [0, 25], 'color': "#E8F5E8"},
                            {'range': [25, 50], 'color': "#FFF3CD"},
                            {'range': [50, 100], 'color': "#F8D7DA"}
                        ],
                        'threshold': {
                            'line': {'color': "darkred", 'width': 4},
                            'thickness': 0.75,
                            'value': 75
                        }
                    },
                    number={'suffix': "%", 'font': {'size': 16}}
                ),
                row=3, col=1
            )
            
            fig_gauges.update_layout(
                height=550,
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='white',
                margin=dict(l=0, r=0, t=60, b=0)
            )
            
            # Update subplot titles to prevent overlap
            for i, annotation in enumerate(fig_gauges.layout.annotations):
                annotation.update(font_size=14, y=annotation.y + 0.02)
            
            st.plotly_chart(fig_gauges, use_container_width=True, key=f"gauges_{region_filter}_{year or 'all'}_{quarter or 'all'}")
        else:
            st.warning("No occupancy rate data available")
    
    # Charts Row 2
    st.markdown("### Organization Analysis")
    
    col1, col2 = st.columns([3, 2])
    
    with col1:
        st.markdown("#### Organization Performance")
        
        tab1, tab2 = st.tabs(["Top Performers", "Bottom Performers"])

        # Create a dynamic title for the charts
        period_title = ""
        if year is not None:
            period_title = f"for {format_financial_year(year)}"
            if quarter is not None:
                period_title += f" {quarter}"

        with tab1:
            if len(filtered_df) > 0 and 'occupancy_rate' in filtered_df.columns:
                top_performers = filtered_df.nlargest(10, 'occupancy_rate')
                num_available = len(top_performers)
                
                # Update title to show actual count if less than 10
                title_text = f"Top {num_available} Organizations by Occupancy Rate {period_title}" if num_available < 10 else f"Top 10 Organizations by Occupancy Rate {period_title}"
                
                fig_top = px.bar(top_performers, 
                                 x='occupancy_rate', 
                                 y='organisation_name', 
                                 orientation='h',
                                 title=title_text,
                                 labels={'occupancy_rate': 'Occupancy Rate (%)', 'organisation_name': 'Organization'},
                                 text='occupancy_rate')
                fig_top.update_traces(texttemplate='%{text:.1f}%', textposition='outside', marker_color='#d62728')
                fig_top.update_layout(height=350, plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', 
                                      font_color='white', yaxis={'categoryorder':'total ascending'},
                                      xaxis_range=[0,105])
                st.plotly_chart(fig_top, use_container_width=True)
            else:
                st.warning("No data available for top performers.")

        with tab2:
            if len(filtered_df) > 0 and 'occupancy_rate' in filtered_df.columns:
                bottom_performers = filtered_df.nsmallest(10, 'occupancy_rate')
                num_available = len(bottom_performers)
                
                # Update title to show actual count if less than 10
                title_text = f"Bottom {num_available} Organizations by Occupancy Rate {period_title}" if num_available < 10 else f"Bottom 10 Organizations by Occupancy Rate {period_title}"
                
                fig_bottom = px.bar(bottom_performers, 
                                    x='occupancy_rate', 
                                    y='organisation_name', 
                                    orientation='h',
                                    title=title_text,
                                    labels={'occupancy_rate': 'Occupancy Rate (%)', 'organisation_name': 'Organization'},
                                    text='occupancy_rate')
                fig_bottom.update_traces(texttemplate='%{text:.1f}%', textposition='outside', marker_color='#2ca02c')
                fig_bottom.update_layout(height=350, plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', 
                                         font_color='white', yaxis={'categoryorder':'total descending'},
                                         xaxis_range=[0,105])
                st.plotly_chart(fig_bottom, use_container_width=True)
            else:
                st.warning("No data available for bottom performers.")

    with col2:
        st.markdown("#### Top Specialities by Occupied Beds")
        
        # Always load speciality data from the API to respect filters
        speciality_df = load_speciality_data(year, quarter)
            
        # Filter speciality data by region if available
        if speciality_df is not None and len(speciality_df) > 0:
            if region_filter != "All Organizations" and 'region_code' in speciality_df.columns:
                speciality_df = speciality_df[speciality_df['region_code'] == region_filter]
        
            
        if speciality_df is not None and len(speciality_df) > 0:
            # Group by speciality and sum occupied beds
            speciality_totals = speciality_df.groupby('speciality')['beds_occupied'].sum().reset_index()
            speciality_totals = speciality_totals.sort_values('beds_occupied', ascending=False).head(10)
            
            if len(speciality_totals) > 0:
                # Use a deterministic key for the pie chart
                chart_key = f"speciality_pie_{region_filter}_{year or 'all'}_{quarter or 'all'}"
                
                fig_pie = go.Figure()
                
                fig_pie.add_trace(go.Pie(
                    labels=speciality_totals['speciality'].tolist(),
                    values=speciality_totals['beds_occupied'].tolist(),
                    textposition='inside',
                    textinfo='percent+label',
                    marker=dict(colors=px.colors.qualitative.Set3[:len(speciality_totals)]),
                    name=f"speciality_beds_{region_filter}"
                ))
                
                fig_pie.update_layout(
                    title=f"Top 10 Specialities by Occupied Beds - {region_filter}",
                    height=350,
                    margin=dict(l=0, r=0, t=40, b=0),
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='white',
                    showlegend=True
                )
                
                st.plotly_chart(fig_pie, use_container_width=True, key=chart_key)
                
                # Show some statistics
                total_speciality_beds = speciality_df['beds_occupied'].sum()
                unique_specialities = len(speciality_df['speciality'].unique())
                st.caption(f"Total occupied beds across specialities: {total_speciality_beds:,}")
                st.caption(f"Number of specialities: {unique_specialities}")
            else:
                st.warning("No speciality data to display for the selected period.")
        else:
            st.warning("No speciality data available for the selected period. Upload an Excel file or check the database.")

def show_upload_page():
    st.header("Data Upload")
    
    tab1, tab2 = st.tabs(["File Upload", "URL Download"])
    
    with tab1:
        st.subheader("Upload NHS Excel File")
        st.markdown("Upload an NHS bed occupancy Excel file to process and store in the database.")
        
        uploaded_file = st.file_uploader(
            "Choose an Excel file", 
            type=['xlsx', 'xls'],
            help="Select an NHS bed occupancy Excel file (.xlsx or .xls)"
        )
        
        if uploaded_file is not None:
            st.success(f"File selected: {uploaded_file.name}")
            
            if st.button("Upload and Process", key="upload_button"):
                with st.spinner("Uploading file and starting processing pipeline..."):
                    result = upload_file_to_api(uploaded_file)
                
                if result:
                    st.success("File uploaded successfully!")
                    st.json(result)
                    
                    if 'flow_run_id' in result:
                        st.info(f"Flow run ID: {result['flow_run_id']}")
                        st.markdown(f"**Status URL**: {result.get('status_url', 'N/A')}")
    
    with tab2:
        st.subheader("Download from URL")
        st.markdown("Provide a URL to download and process NHS bed occupancy data.")
        
        # Use session state to manage URL input and processing state
        if 'url_processing' not in st.session_state:
            st.session_state.url_processing = False
        
        url_input = st.text_input(
            "Enter URL",
            placeholder="https://example.com/nhs-data.xlsx",
            help="Enter the URL of an NHS bed occupancy Excel file",
            key="url_input_field"
        )
        
        if url_input and st.button("Download and Process", key="download_button"):
            st.session_state.url_processing = True
            with st.spinner("Triggering pipeline for URL download..."):
                result = trigger_pipeline(url_input)
            
            if result:
                st.success("Pipeline triggered successfully!")
                st.json(result)
                
                if 'flow_run_id' in result:
                    st.info(f"Flow run ID: {result['flow_run_id']}")
                    st.markdown(f"**Status URL**: {result.get('status_url', 'N/A')}")
                
                # Add a button to process another URL
                if st.button("Process Another URL", key="process_another_url"):
                    st.session_state.url_processing = False
                    st.rerun()

def show_pipeline_status():
    st.header("Pipeline Status")
    st.markdown("Check the status of your data processing pipelines.")
    
    flow_run_id = st.text_input(
        "Flow Run ID",
        placeholder="Enter flow run ID to check status",
        help="Enter the flow run ID from a previous upload or pipeline trigger"
    )
    
    if flow_run_id and st.button("Check Status", key="check_status_button"):
        with st.spinner("Fetching flow status..."):
            status = get_flow_status(flow_run_id)
        
        if status:
            st.subheader("Flow Run Details")
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Flow Run ID", status.get('flow_run__id', 'N/A'))
                st.metric("Name", status.get('name', 'N/A'))
                st.metric("State", status.get('state', 'Unknown'))
            
            with col2:
                is_completed = status.get('is_completed', False)
                is_failed = status.get('is_failed', False)
                
                if is_completed:
                    st.success("Completed")
                elif is_failed:
                    st.error("Failed")
                else:
                    st.info("In Progress")
                
                st.metric("Created", status.get('created_at', 'N/A'))
                st.metric("Updated", status.get('updated_at', 'N/A'))
            
            if status.get('message'):
                st.subheader("Message")
                st.text(status['message'])



if __name__ == "__main__":
    main()