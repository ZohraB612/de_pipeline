import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
from typing import Dict, List, Any
import json

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
        response = requests.post(api_url, json={"url": url})
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Pipeline trigger failed: {str(e)}")
        return {}

def get_flow_status(flow_run_id: str) -> Dict[str, Any]:
    """Get status of a specific flow run"""
    return make_api_request(f"/nhs-data/status/{flow_run_id}")

def main():
    st.set_page_config(
        page_title="NHS Bed Occupancy Dashboard",
        page_icon="🏥",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    st.title("🏥 NHS Bed Occupancy Analytics Dashboard")
    st.markdown("### Monitor and analyze NHS bed occupancy data across organizations")

    # Sidebar for navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.radio(
        "Select Page",
        ["📊 Dashboard", "📤 Data Upload", "🔄 Pipeline Status", "🔍 Organization Search"]
    )

    if page == "📊 Dashboard":
        show_dashboard()
    elif page == "📤 Data Upload":
        show_upload_page()
    elif page == "🔄 Pipeline Status":
        show_pipeline_status()
    elif page == "🔍 Organization Search":
        show_organization_search()

def show_dashboard():
    st.header("📊 NHS Bed Occupancy Dashboard")
    
    # Fetch occupancy data
    with st.spinner("Loading occupancy data..."):
        data = make_api_request("/nhs-data/occupancy")
    
    if not data or not isinstance(data, list):
        st.warning("No occupancy data available. Please upload data or trigger the pipeline first.")
        return
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    if df.empty:
        st.warning("No data found in the database.")
        return
    
    # Display metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Organizations", len(df['organisation_name'].unique()))
    
    with col2:
        avg_occupancy = df['occupancy_rate'].mean()
        st.metric("Average Occupancy Rate", f"{avg_occupancy:.1f}%")
    
    with col3:
        max_occupancy = df['occupancy_rate'].max()
        st.metric("Highest Occupancy Rate", f"{max_occupancy:.1f}%")
    
    with col4:
        min_occupancy = df['occupancy_rate'].min()
        st.metric("Lowest Occupancy Rate", f"{min_occupancy:.1f}%")
    
    # Charts
    st.subheader("📈 Visualizations")
    
    # Occupancy rate distribution
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Occupancy Rate Distribution")
        fig_hist = px.histogram(
            df, 
            x='occupancy_rate', 
            nbins=20,
            title="Distribution of Occupancy Rates",
            labels={'occupancy_rate': 'Occupancy Rate (%)', 'count': 'Number of Organizations'}
        )
        st.plotly_chart(fig_hist, use_container_width=True)
    
    with col2:
        st.subheader("Top 10 Organizations by Occupancy Rate")
        top_10 = df.nlargest(10, 'occupancy_rate')
        fig_bar = px.bar(
            top_10,
            x='occupancy_rate',
            y='organisation_name',
            orientation='h',
            title="Top 10 Organizations by Occupancy Rate",
            labels={'occupancy_rate': 'Occupancy Rate (%)', 'organisation_name': 'Organization'}
        )
        st.plotly_chart(fig_bar, use_container_width=True)
    
    # Data table
    st.subheader("📋 Raw Data")
    
    # Add filters
    col1, col2 = st.columns(2)
    with col1:
        min_occupancy_filter = st.slider("Minimum Occupancy Rate (%)", 0, 100, 0)
    with col2:
        max_occupancy_filter = st.slider("Maximum Occupancy Rate (%)", 0, 100, 100)
    
    filtered_df = df[
        (df['occupancy_rate'] >= min_occupancy_filter) & 
        (df['occupancy_rate'] <= max_occupancy_filter)
    ]
    
    st.dataframe(
        filtered_df.sort_values('occupancy_rate', ascending=False),
        use_container_width=True
    )

def show_upload_page():
    st.header("📤 Data Upload")
    
    tab1, tab2 = st.tabs(["📁 File Upload", "🌐 URL Download"])
    
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
            
            if st.button("Upload and Process"):
                with st.spinner("Uploading file and starting processing pipeline..."):
                    result = upload_file_to_api(uploaded_file)
                
                if result:
                    st.success("✅ File uploaded successfully!")
                    st.json(result)
                    
                    if 'flow_run_id' in result:
                        st.info(f"Flow run ID: {result['flow_run_id']}")
                        st.markdown(f"**Status URL**: {result.get('status_url', 'N/A')}")
    
    with tab2:
        st.subheader("Download from URL")
        st.markdown("Provide a URL to download and process NHS bed occupancy data.")
        
        url_input = st.text_input(
            "Enter URL",
            placeholder="https://example.com/nhs-data.xlsx",
            help="Enter the URL of an NHS bed occupancy Excel file"
        )
        
        if url_input and st.button("Download and Process"):
            with st.spinner("Triggering pipeline for URL download..."):
                result = trigger_pipeline(url_input)
            
            if result:
                st.success("✅ Pipeline triggered successfully!")
                st.json(result)
                
                if 'flow_run_id' in result:
                    st.info(f"Flow run ID: {result['flow_run_id']}")
                    st.markdown(f"**Status URL**: {result.get('status_url', 'N/A')}")

def show_pipeline_status():
    st.header("🔄 Pipeline Status")
    st.markdown("Check the status of your data processing pipelines.")
    
    flow_run_id = st.text_input(
        "Flow Run ID",
        placeholder="Enter flow run ID to check status",
        help="Enter the flow run ID from a previous upload or pipeline trigger"
    )
    
    if flow_run_id and st.button("Check Status"):
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
                    st.success("✅ Completed")
                elif is_failed:
                    st.error("❌ Failed")
                else:
                    st.info("⏳ In Progress")
                
                st.metric("Created", status.get('created_at', 'N/A'))
                st.metric("Updated", status.get('updated_at', 'N/A'))
            
            if status.get('message'):
                st.subheader("Message")
                st.text(status['message'])

def show_organization_search():
    st.header("🔍 Organization Search")
    st.markdown("Search for specific organizations and view their bed occupancy data.")
    
    search_type = st.radio(
        "Search by:",
        ["Organization Code", "Organization Name"]
    )
    
    if search_type == "Organization Code":
        org_code = st.text_input(
            "Enter Organization Code",
            placeholder="e.g., RWE",
            help="Enter the NHS organization code"
        )
        
        if org_code and st.button("Search by Code"):
            with st.spinner(f"Searching for organization code: {org_code}"):
                data = make_api_request(f"/nhs-data/organization/{org_code}")
            
            if data and 'data' in data:
                display_organization_data(data)
            else:
                st.warning(f"No data found for organization code: {org_code}")
    
    elif search_type == "Organization Name":
        org_name = st.text_input(
            "Enter Organization Name (partial match)",
            placeholder="e.g., Royal",
            help="Enter part of the organization name"
        )
        
        if org_name and st.button("Search by Name"):
            with st.spinner(f"Searching for organizations containing: {org_name}"):
                data = make_api_request(f"/nhs-data/organization-name/{org_name}")
            
            if data and 'data' in data:
                display_organization_data(data)
            else:
                st.warning(f"No organizations found containing: {org_name}")

def display_organization_data(data):
    """Display organization data in a formatted way"""
    st.success("✅ Organizations found!")
    
    # Display summary metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Records", data.get('total_records', 0))
    with col2:
        if 'organizations_found' in data:
            st.metric("Organizations Found", data['organizations_found'])
    with col3:
        if 'organisation_name' in data:
            st.metric("Organization", data['organisation_name'])
    
    # Convert to DataFrame and display
    df = pd.DataFrame(data['data'])
    
    if not df.empty:
        st.subheader("📊 Organization Data")
        
        # Summary statistics
        if len(df) > 1:
            avg_occupancy = df['occupancy_rate'].mean()
            max_occupancy = df['occupancy_rate'].max()
            min_occupancy = df['occupancy_rate'].min()
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Average Occupancy", f"{avg_occupancy:.1f}%")
            with col2:
                st.metric("Maximum Occupancy", f"{max_occupancy:.1f}%")
            with col3:
                st.metric("Minimum Occupancy", f"{min_occupancy:.1f}%")
        
        # Data table
        st.dataframe(df, use_container_width=True)
        
        # Chart if multiple records
        if len(df) > 1:
            st.subheader("📈 Occupancy Rate by Organization")
            fig = px.bar(
                df,
                x='organisation_name',
                y='occupancy_rate',
                title="Occupancy Rate by Organization",
                labels={'occupancy_rate': 'Occupancy Rate (%)', 'organisation_name': 'Organization'}
            )
            fig.update_xaxis(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()