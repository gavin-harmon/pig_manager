import streamlit as st
import numpy as np
import pandas as pd
import duckdb
from datetime import datetime
import pytz
import ftplib
import io
import os
import glob
import sys
import urllib.parse
from azure.storage.blob import BlobServiceClient
from configparser import ConfigParser

# Import feature modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))
from streamlit_feature_catalog.features.table_filters.var_002 import DataFilter, DataFilterConfig
from streamlit_feature_catalog.features.blob_view.var_001 import BlobViewManager
from streamlit_feature_catalog.features.category_manager.var_001 import CategoryManager, CategoryManagerConfig
from streamlit_feature_catalog.features.blob_navigation.var_003 import BlobNavigator, BlobNavigatorConfig

# Azure Storage configuration
AZURE_ACCOUNT_URL = st.secrets["AZ_ACCOUNT"]
CONTAINER_NAME = st.secrets["AZ_CONTAINER"]

# Page configuration
st.set_page_config(
    page_title="Salsify PIG Manager",
    layout="wide",
    initial_sidebar_state="expanded" )

# Define the exact column order
COLUMN_ORDER = [
    'Item', 'Category', 'About', 'Bullet Copy', 'Heading', 'Spanish Bullet Copy',
    'Subheading', 'Enhanced Product Name', 'Bullet Copy 1', 'Bullet Copy 2',
    'Bullet Copy 3', 'Bullet Copy 4', 'Bullet Copy 5', 'Bullet Copy 6',
    'Bullet Copy 7', 'Bullet Copy 8', 'Bullet Copy 9', 'Bullet Copy 10',
    'Feature/Benefit 1', 'Feature/Benefit 2', 'FeatureBenefit 3', 'Feature/Benefit 4',
    'FeatureBenefit 5', 'Feature/Benefit 6', 'Feature/Benefit 7', 'Feature/Benefit 8',
    'Feature/Benefit 9', 'Feature/Benefit 10', 'Keywords', 'Long Description',
    'Product ID', 'Product Title', 'SEO Enhanced Bullets 1', 'SEO Enhanced Bullets 2',
    'SEO Enhanced Bullets 3', 'SEO Enhanced Bullets 4', 'SEO Enhanced Bullets 5',
    'SEO Enhanced Bullets 6', 'SEO Enhanced Bullets 7', 'SEO Enhanced Bullets 8',
    'SEO Enhanced Bullets 9', 'SEO Enhanced Bullets 10', 'Short Description',
    'USP', 'Brand'
]

# Mapping definitions
FIELD_MAPPINGS = {
    # Core fields mapped from direct Excel references
    'Item': ('B3', 'not in pig'),  # Model Number
    'Product ID': ('B3', 'not in pig'),  # Model Number
    'Brand': ('B2', 'not in pig'),
    'Product Title': ('B4', 'not in pig'),  # Product Name
    'Enhanced Product Name': ('B5', 'not in pig'),
    'USP': ('B6', 'not in pig'),
    'Short Description': ('B8', 'not in pig'),
    'Long Description': ('B9', 'not in pig'),
    'Keywords': ('B10', 'not in pig'),
}

# Generate dynamic mappings for additional fields
BULLET_MAPPINGS = {f'Bullet Copy {i}': (f'A{10+i}', 'not in pig') for i in range(1, 11)}
# Define Feature/Benefit mappings with special handling for 3 and 5
FEATURE_BENEFIT_MAPPINGS = {
    **{f'Feature/Benefit {i}': (f'B{10+i}', 'not in pig') for i in range(1, 3)},  # 1-2
    'FeatureBenefit 3': ('B13', 'not in pig'),                                     # 3
    'Feature/Benefit 4': ('B14', 'not in pig'),                                    # 4
    'FeatureBenefit 5': ('B15', 'not in pig'),                                    # 5
    **{f'Feature/Benefit {i}': (f'B{10+i}', 'not in pig') for i in range(6, 11)}  # 6-10
}
SEO_BULLET_MAPPINGS = {f'SEO Enhanced Bullets {i}': (f'C{10+i}', 'not in pig') for i in range(1, 11)}


# Combine all mappings
ALL_MAPPINGS = {
    **FIELD_MAPPINGS,
    **BULLET_MAPPINGS,
    **FEATURE_BENEFIT_MAPPINGS,
    **SEO_BULLET_MAPPINGS,
}

def initialize_session_state():
    """Initialize session state variables"""
    if 'current_view' not in st.session_state:
        st.session_state.current_view = "About"
    if 'df' not in st.session_state:
        st.session_state.df = None
    if 'con' not in st.session_state:
        st.session_state.con = None
    if 'filtered_df' not in st.session_state:
        st.session_state.filtered_df = None
    if 'uploaded_pig' not in st.session_state:
        st.session_state.uploaded_pig = None
    if 'category_values' not in st.session_state:
        st.session_state.category_values = None
    if 'status_values' not in st.session_state:
        st.session_state.status_values = None
    if 'data_loaded' not in st.session_state:
        st.session_state.data_loaded = False
    if 'selected_category' not in st.session_state:
        st.session_state.selected_category = None
    if 'selected_status' not in st.session_state:
        st.session_state.selected_status = None
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    if 'sas_token' not in st.session_state:
        st.session_state.sas_token = None


def get_blob_service_client(sas_token):
    """Create BlobServiceClient using SAS token"""
    return BlobServiceClient(
        account_url=AZURE_ACCOUNT_URL,
        credential=sas_token  # Use token as-is
    )



def get_filtered_blob_navigator(prefix, key_prefix):
    """Helper function to create a configured BlobNavigator"""
    # Make sure the prefix uses forward slashes and ends with a slash
    normalized_prefix = prefix.replace('\\', '/').rstrip('/') + '/'

    return BlobNavigator(
        config=BlobNavigatorConfig(
            account_url=AZURE_ACCOUNT_URL,
            sas_token=st.session_state.sas_token,
            allowed_containers=[st.secrets["AZ_CONTAINER"]],
            path_prefix=normalized_prefix,
            allowed_extensions=['.xlsx'],  # Only show Excel files
            show_file_details=True,
            timezone='US/Eastern',
            placeholder_text=f"Search for {'PIG' if 'pig' in prefix else 'Salsify'} files..."
        ),
        key_prefix=key_prefix
    )

def show_about_page():
    st.markdown("""
    # Salsify Product Information Manager

    ## Quick Start Guide
    
    ### New PIG Upload Process:
    1. Navigate to "**PIG Management**" in the sidebar
    2. Upload your PIG Excel file using the file uploader
    3. Click "**Update in-app Salsify view**" to process your PIG
    4. Navigate to "**Upload to Salsify**" in the sidebar
    5. Click "**Preview Session Records**" to review your changes
    6. Click "**Upload to Salsify**" to finalize and upload

    ## Key Features

    ### PIG Management
    - Upload and process new Product Information Guides (PIGs)
    - Preview and edit content before saving
    - Automatic backup of PIGs in the repository
    - Access previous versions when needed

    ### Data Viewing and Filtering
    - Browse through all product information
    - Filter products by category or status
    - Export filtered data for offline use
    - View category statistics and distributions

    ### Salsify Integration
    - Preview changes before uploading
    - Automatic backup creation
    - Direct upload to Salsify SFTP
    - Historical version tracking

    ## Need Help?
    - Technical support: gavin.harmon@hamiltonbeach.com
    - Product team questions: Contact your team lead
    - Salsify access: Check with your Salsify administrator
    """)

    # Add usage tips in an expander
    with st.expander("Best Practices"):
        st.markdown("""
        - Always preview your data before uploading to Salsify
        - Check category assignments carefully
        - Use filters to verify your changes
        - Download backups of important data
        - Verify PIG content in preview before processing
        """)

    return True


def show_sidebar(con=None):
    """Display dynamic sidebar based on current view"""
    with st.sidebar:
        # View selection at the top of sidebar
        previous_view = st.session_state.current_view
        st.session_state.current_view = st.radio(
            "Page Navigation",
            ["About", "View Salsify Data", "PIG Management", "Upload To Salsify"],
            key="page_navigation"
        )

        # Clear PIG processing data when navigating away from PIG Management
        if previous_view == "PIG Management" and st.session_state.current_view != "PIG Management":
            keys_to_clear = [
                'uploaded_pig',
                'temp_pig_mapped',
                'filtered_df',
                'selected_category',
                'selected_status'
            ]
            for key in keys_to_clear:
                if key in st.session_state:
                    del st.session_state[key]

        st.divider()

        # Dynamic sidebar content based on current view
        if st.session_state.current_view == "About":
            pass  # No additional sidebar content for About page

        elif st.session_state.current_view == "View Salsify Data":
            show_data_filters()

        elif st.session_state.current_view == "PIG Management":
            # Use the filtered navigator
            navigator = get_filtered_blob_navigator('pig-repository/', 'pig_nav')
            selected_file = navigator.render_navigation()

            # Add Load Selected PIG button
            if selected_file:
                if st.button("Load Selected PIG", use_container_width=True):
                    try:
                        file_content = navigator.load_file_content(selected_file)
                        if file_content:
                            file_obj = io.BytesIO(file_content)
                            file_obj.name = selected_file.split('/')[-1]
                            file_obj.type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            st.session_state.uploaded_pig = file_obj
                            st.success(f"Successfully loaded: {file_obj.name}")
                            st.rerun()
                        else:
                            st.error("Could not load the selected file")
                    except Exception as e:
                        st.error(f"Error loading PIG file: {str(e)}")

            if st.button("Manage Categories", use_container_width=True):
                st.session_state.show_category_manager = True
                
            if 'uploaded_pig' in st.session_state and st.session_state.uploaded_pig:
                st.divider()
                preview_df = pd.read_excel(st.session_state.uploaded_pig, header=None)
                item_number = preview_df.iloc[2, 1]  # B3 contains the Item number
                st.session_state.uploaded_pig.seek(0)
                st.download_button(
                    label=f"📥 Download {item_number}_PIG.xlsx",
                    data=st.session_state.uploaded_pig.getvalue(),
                    file_name=f"{item_number}_PIG.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )                

        elif st.session_state.current_view == "Upload To Salsify":
            # Use the filtered navigator
            navigator = get_filtered_blob_navigator('salsify-sftp/', 'salsify_nav')
            selected_file = navigator.render_navigation()

            # Add buttons for preview options
            if selected_file:
                if st.button("Preview Selected File", use_container_width=True):
                    try:
                        file_content = navigator.load_file_content(selected_file)
                        if file_content:
                            file_obj = io.BytesIO(file_content)
                            df = pd.read_excel(file_obj)
                            st.session_state.preview_df = df
                            st.session_state.preview_filename = selected_file.split('/')[-1]
                            st.success(f"Loaded {st.session_state.preview_filename} for preview")
                            st.rerun()
                    except Exception as e:
                        st.error(f"Error loading file: {str(e)}")

            if st.button("Preview Session Records", use_container_width=True):
                try:
                    if con is not None:
                        session_df = con.execute("""
                            SELECT DISTINCT * EXCLUDE(Status) 
                            FROM pig_data 
                            ORDER BY Item
                        """).df()

                        st.session_state.preview_df = session_df
                        st.session_state.preview_filename = "Current Session Data"
                        st.success("Loaded current session records for preview")
                        st.rerun()
                    else:
                        st.error("Database connection not available")
                except Exception as e:
                    st.error(f"Error loading session data: {str(e)}")

            # Show filter options if a file is being previewed
            if hasattr(st.session_state, 'preview_df'):
                st.divider()
                st.markdown(f"**Previewing:** {st.session_state.preview_filename}")

                # Initialize filter configuration
                filter_config = DataFilterConfig(
                    allowed_extensions=['.parquet', '.csv', '.xlsx'],
                    show_metrics=True
                )

                # Initialize filter interface with unique key for sidebar
                filter_interface = DataFilter(
                    data=st.session_state.preview_df,
                    config=filter_config,
                    session_key="salsify_sidebar_filter"
                )

                # Render filter controls in sidebar and store results in session state
                st.session_state.filtered_preview_df = filter_interface.render()

                st.divider()
                excel_buffer = io.BytesIO()
                st.session_state.preview_df.to_excel(excel_buffer, index=False, engine='openpyxl')
                excel_buffer.seek(0)
                
                st.download_button(
                    label="📥 Download Salsify Data",
                    data=excel_buffer.getvalue(),
                    file_name="salsify_export.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )


def show_data_filters():
    """Show data filters in sidebar for View Salsify Data"""

    # Check if data needs to be loaded
    if st.session_state.df is None and st.session_state.con is not None:
        # Load data into DataFrame if not already loaded
        df = st.session_state.con.execute("SELECT distinct * FROM pig_data").df()
        st.session_state.df = df

    if st.session_state.df is not None:
        # Initialize filter configuration
        filter_config = DataFilterConfig(
            allowed_extensions=['.parquet', '.csv', '.xlsx'],
            show_metrics=True
        )

        # Initialize filter interface with unique key
        filter_interface = DataFilter(
            data=st.session_state.df,
            config=filter_config,
            session_key="sidebar_pig_filter"
        )

        # Render filter interface and get filtered data
        st.session_state.filtered_df = filter_interface.render()

def validate_product_name_length(df, max_length=100):
    """
    Validate that the Product Name in cell B4 is not more than max_length characters.
    
    Args:
        df: Pandas DataFrame containing the Excel file data
        max_length: Maximum allowed length for Product Name
        
    Returns:
        tuple: (is_valid, error_message)
    """
    try:
        # Extract Product Name from cell B4
        product_name = str(df.iloc[3, 1])  # B4 in zero-indexed DataFrame
        
        # Check if product name is too long
        if len(product_name) > max_length:
            return False, f"Product Name is too long ({len(product_name)} characters). Maximum allowed is {max_length} characters. Check if there's a second line visible in the formula bar."
        
        return True, ""
    except Exception as e:
        return False, f"Error validating Product Name: {str(e)}"

def show_upload_controls():
    """Show upload controls in sidebar for Upload New PIG"""
    st.sidebar.header("Upload Controls")

    uploaded_file = st.sidebar.file_uploader("Upload PIG File", type=['xlsx'])

    if uploaded_file:
        st.session_state.uploaded_pig = uploaded_file

        # Add category and status selection
        st.sidebar.subheader("Set Category")
        st.session_state.selected_category = st.sidebar.selectbox(
            "Select Category",
            options=st.session_state.con.execute(
                "SELECT DISTINCT category_value FROM category_values order by 1"
            ).df()['category_value'].unique()
        )

        st.sidebar.subheader("Set Status")
        st.session_state.selected_status = st.sidebar.selectbox(
            "Select Status",
            options=st.session_state.con.execute(
                "SELECT DISTINCT status_values FROM status_values order by 1"
            ).df()['status_values'].unique()
        )
def main_content():
    """Display main content based on current view"""
    if st.session_state.current_view == "About":
        show_about_page()
    elif st.session_state.current_view == "View Salsify Data":
        show_data_view(st.session_state.con)
    elif st.session_state.current_view == "PIG Management":
        if getattr(st.session_state, 'show_category_manager', False):
            show_category_management()
        else:
            show_upload_interface(st.session_state.con)
    elif st.session_state.current_view == "Upload To Salsify":
        show_salsify_upload(st.session_state.con)

def extract_cell_value(df, cell_reference):
    """
    Extract value from Excel cell reference (e.g., 'B3')
    Handles missing values and formatting
    """
    if not cell_reference:
        return 'not in pig'

    try:
        col = cell_reference[0]
        row = int(cell_reference[1:]) - 1
        value = df.iloc[row][ord(col) - ord('A')]

        # Handle NaN/None values
        if pd.isna(value):
            return 'not in pig'

        # Convert to string and handle any special characters
        return str(value).strip()
    except (IndexError, ValueError):
        return 'not in pig'


def download_from_blob(sas_token, container_name, blob_name, local_dir, filename):
    """Download file from Azure Blob Storage using SAS token"""
    try:
        # Create the BlobServiceClient using SAS token
        account_url = "https://daorgshare.blob.core.windows.net"
        # Create blob service client with SAS token
        blob_service_client = BlobServiceClient(
            account_url=f"{account_url}?{sas_token.lstrip('?')}"
        )

        # Get a blob client
        blob_client = blob_service_client.get_container_client(container_name).get_blob_client(blob_name)

        # Create full local path
        local_path = os.path.join(local_dir, filename)

        # Ensure directory exists
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        # Download the file
        with open(local_path, "wb") as file:
            blob_data = blob_client.download_blob()
            file.write(blob_data.readall())

        return True, local_path
    except Exception as e:
        st.error(f"Error downloading {blob_name}: {str(e)}")
        return False, None

def upload_to_blob(sas_token, container_name, blob_name, file_path):
    """Upload file to Azure Blob Storage"""
    try:
        blob_service_client = get_blob_service_client(sas_token)
        blob_client = blob_service_client.get_container_client(container_name).get_blob_client(blob_name)

        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        return True
    except Exception as e:
        st.error(f"Error uploading {blob_name}: {str(e)}")
        return False


def load_local_data():
    """Load data from local files into DuckDB"""
    try:
        con = duckdb.connect(database=':memory:')

        # Load all parquet files without Azure connection
        parquet_files = glob.glob("local_pig-info-table-*.parquet")
        if not parquet_files:
            st.error("No local parquet files found. Please download data first.")
            return None

        # Create combined table from all parquet files
        query = f"""
        CREATE OR REPLACE TABLE pig_data AS 
        SELECT * FROM read_parquet({", ".join(repr(f) for f in parquet_files)})
        """
        con.execute(query)

        # Load reference data from local files
        con.execute("""
        CREATE OR REPLACE TABLE category_values AS 
        SELECT * FROM read_csv('local_category_values.csv', header=true) order by 2 
        """)

        con.execute("""
        CREATE OR REPLACE TABLE status_values AS 
        SELECT * FROM read_csv('local_status_values.csv', header=true) order by 1
        """)

        return con
    except Exception as e:
        st.error(f"Error loading local data: {str(e)}")
        return None


def initialize_category_manager():
    """Initialize the category manager with configuration"""
    try:
        # Set up the path
        base_dir = os.path.join(os.getcwd(), 'local_data')
        reference_dir = os.path.join(base_dir, 'reference')        
        local_path = f'az://{st.secrets["AZ_CONTAINER"]}/salsify-product-info/app-data/validation/category_values.csv'

        # Create directories if they don't exist
        os.makedirs(reference_dir, exist_ok=True)

        # Simple configuration - removed track_changes
        config = CategoryManagerConfig(
            source_path=local_path,
            file_type="csv",
            require_unique_rows=True
        )

        return CategoryManager(config=config)

    except Exception as e:
        st.error(f"Error initializing category manager: {str(e)}")
        return None


def show_category_management():
    """Show the category and status management interface with back button"""
    col1, col2 = st.columns([1, 11])
    with col1:
        if st.button("← Back", use_container_width=True):
            st.session_state.show_category_manager = False
            st.rerun()

    with col2:
        st.header("Reference Data Management")

    data_type = st.radio(
        "Select Type to Manage",
        ["Categories", "Status Values"],
        horizontal=True,
        key="reference_data_type"
    )

    try:
        blob_path = (
            f'{st.secrets["AZ_CONTAINER"]}/salsify-product-info/app-data/validation/category_values.csv'
            if data_type == "Categories"
            else f'{st.secrets["AZ_CONTAINER"]}/salsify-product-info/app-data/validation/status_values.csv'
        )

        # Remove connection string creation and use SAS token directly
        manager = CategoryManager(
                config=CategoryManagerConfig(
                    source_path=blob_path,
                    file_type="csv",
                    require_unique_rows=True
                ),
                sas_token=st.session_state.sas_token  # Pass SAS token here
            )
        manager.render_manager()

    except Exception as e:
        st.error(f"Error in reference data management: {str(e)}")

def download_all_files(sas_token):  # Change parameter name from connection_string to sas_token
    """Download all required files from blob storage"""
    status_files = {
        'active': 'app-data/pig-info-table.parquet/Status=active/data_0.parquet',
        'New': 'app-data/pig-info-table.parquet/Status=New/data_0.parquet',
        'Obsolete': 'app-data/pig-info-table.parquet/Status=Obsolete/data_0.parquet'
    }

    for status, blob_path in status_files.items():
        success = download_from_blob(
            sas_token,
            st.secrets["AZ_CONTAINER"],
            blob_path,
            "local_data",
            f"pig-info-table-{status}.parquet"
        )
        if not success:
            return False

    return True


def show_data_view(con):
    """Show existing PIG data with filtering and export options"""
    st.header("View Salsify Data")
    try:
        # Initialize view manager
        view_manager = BlobViewManager()

        # Use existing filtered_df or df from session state
        if 'filtered_df' not in st.session_state or st.session_state.filtered_df is None:
            st.session_state.filtered_df = st.session_state.df

        filtered_df = st.session_state.filtered_df

        if filtered_df is not None and not filtered_df.empty:
            # Show summary statistics
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Records", len(filtered_df))
            with col2:
                st.metric("Categories", filtered_df['Category'].nunique() if 'Category' in filtered_df.columns else 0)
            with col3:
                st.metric("Status Types", filtered_df['Status'].nunique() if 'Status' in filtered_df.columns else 0)

            # Category breakdown
            if 'Category' in filtered_df.columns:
                st.subheader("Records by Category")
                category_counts = filtered_df['Category'].value_counts()
                st.bar_chart(category_counts)

            # Display filtered data
            st.header("Salsify Data")
            st.dataframe(filtered_df)

            # Export functionality
            st.download_button(
                label="Download Displayed Data (CSV)",
                data=filtered_df.to_csv(index=False),
                file_name="pig_data_export.csv",
                mime="text/csv"
            )
        else:
            st.info("No data available to display. Please check your filters or data loading.")

    except Exception as e:
        st.error(f"Error displaying data: {str(e)}")

def process_pig_file(uploaded_file, con):
    """Process uploaded PIG file and store in DuckDB"""
    try:
        # Read Excel file WITHOUT headers, exactly like the old version
        df = pd.read_excel(
            uploaded_file,
            header=None,  # This is crucial!
            engine='openpyxl'
        )

        # Initialize output data with default values
        output_data = {field: 'not in pig' for field in COLUMN_ORDER}

        # Track processed fields for validation
        processed_fields = set()

        # Map fields based on cell references using the old version's logic
        for field, (cell_ref, default) in ALL_MAPPINGS.items():
            if cell_ref:
                value = extract_cell_value(df, cell_ref)
                output_data[field] = value
                processed_fields.add(field)

        # Create validation info
        missing_fields = set(ALL_MAPPINGS.keys()) - processed_fields
        validation_info = {
            'processed_fields': len(processed_fields),
            'missing_fields': len(missing_fields),
            'total_fields': len(ALL_MAPPINGS)
        }

        # Store mapped data in DuckDB
        mapped_df = pd.DataFrame([output_data])
        con.execute("DROP TABLE IF EXISTS temp_pig_mapped")
        con.execute("CREATE TABLE temp_pig_mapped AS SELECT * FROM mapped_df")

        return True, validation_info

    except Exception as e:
        st.error(f"Error processing file: {str(e)}")
        return False, None



def show_upload_interface(con):
    """Show PIG file upload interface with view and validation"""
    st.header("Product Information Guide Management")

    # File uploader
    uploaded_file = st.file_uploader("Upload PIG File", type=['xlsx'], key="pig_file_uploader")

    # Use either the uploaded file or the one from session state
    file_to_process = uploaded_file if uploaded_file is not None else st.session_state.uploaded_pig

    if file_to_process:
        try:
            # Show file preview before processing (ONLY ONCE)
            with st.expander("View PIG File", expanded=False):
                st.info("Viewing uploaded file content. Verify the data before processing.")
                preview_df = pd.read_excel(file_to_process, header=None, engine='openpyxl')
                st.dataframe(preview_df)
                file_to_process.seek(0)  # Reset file pointer after reading
            
            # Add validation here
            is_valid, error_message = validate_product_name_length(preview_df)
            if not is_valid:
                st.error(error_message)
                return  # Exit early if validation fails
            
            # Get Item number for file naming
            item_number = preview_df.iloc[2, 1]  # B3 contains the Item number
            
            # Add save to repository button
            if st.button("Save xlsx to PIG Repository"):
                try:
                    # Create blob client using SAS token
                    blob_service_client = BlobServiceClient(
                        account_url=AZURE_ACCOUNT_URL,
                        credential=st.session_state.sas_token
                    )
                    shared_container_client = blob_service_client.get_container_client(st.secrets["AZ_CONTAINER"])
                
                    # Set filename using new format
                    item_number = preview_df.iloc[2, 1]  # B3 contains the Item number
                    blob_name = f"pig-repository/{item_number} - PIG.xlsx"  # New naming format
                
                    # Upload file to container
                    blob_client = shared_container_client.get_blob_client(blob_name)
                    file_to_process.seek(0)  # Reset file pointer
                    blob_client.upload_blob(file_to_process.read(), overwrite=True)
                    file_to_process.seek(0)  # Reset file pointer for further processing
                
                    st.success(f"✅ Successfully saved {blob_name} to PIG Repository!")
                except Exception as e:
                    st.error(f"Error saving to PIG Repository: {str(e)}")

            # Process the file into DuckDB
            success, validation_info = process_pig_file(file_to_process, con)


            if success:
                # Summary metrics
                st.subheader("PIG Summary")
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Processed Fields", validation_info['processed_fields'])
                with col2:
                    st.metric("Missing Fields", validation_info['missing_fields'])
                with col3:
                    st.metric("Total Fields", validation_info['total_fields'])

                # Get mapped data
                mapped_data = con.execute("SELECT * FROM temp_pig_mapped LIMIT 1").df().iloc[0]

                # Show field groups
                st.subheader("PIG Upload Information - Editable")
                field_groups = {
                    "Core Information": ['Item', 'Product ID', 'Brand', 'Enhanced Product Name', 'Product Title'],
                    "Descriptions": ['Short Description', 'Long Description', 'USP', 'Keywords'],
                    "Bullet Points": [f'Bullet Copy {i}' for i in range(1, 11)],
                    "Features/Benefits": [f'Feature/Benefit {i}' for i in range(1, 2)] + ['FeatureBenefit 3',
                                                                                          'Feature/Benefit 4',
                                                                                          'FeatureBenefit 5'] + [
                                             f'Feature/Benefit {i}' for i in range(6, 11)],
                    "SEO Content": [f'SEO Enhanced Bullets {i}' for i in range(1, 11)]
                }

                tabs = st.tabs(list(field_groups.keys()))
                for tab, (group_name, fields) in zip(tabs, field_groups.items()):
                    with tab:
                        for field in fields:
                            col1, col2 = st.columns([1, 3])
                            with col1:
                                st.write(f"{field}:")
                            with col2:
                                value = mapped_data[field]
                                if value == 'not in pig':
                                    st.write("🚫 Not in PIG")
                                else:
                                    st.write(value)

                # Category and status selection
                col1, col2 = st.columns(2)
                with col1:
                    st.subheader("Set Category")
                    category = st.selectbox(
                        "Select Category",
                        options=con.execute(
                            "SELECT DISTINCT category_value FROM category_values order by 1"
                        ).df()['category_value'].unique(),
                        key='category_select'
                    )

                with col2:
                    st.subheader("Set Status")
                    status = st.selectbox(
                        "Select Status",
                        options=con.execute(
                            "SELECT DISTINCT status_values FROM status_values order by 1"
                        ).df()['status_values'].unique(),
                        key='status_select'
                    )

                # Data View
                st.subheader("Data View")
                df = con.execute("SELECT * FROM temp_pig_mapped").df()
                st.write("Debug - Original DataFrame from temp_pig_mapped:")
                st.write(f"Shape: {df.shape}")
                st.write(f"Items: {df['Item'].tolist()}")

                edited_df = st.data_editor(
                    df,
                    use_container_width=True,
                    hide_index=True,
                    num_rows="dynamic"
                )

                # Debug info
                st.write("Debug - Edited DataFrame:")
                st.write(f"Shape: {edited_df.shape}")
                st.write(f"Items in edited_df: {edited_df['Item'].tolist()}")
                st.write(f"First item using iloc[0]: {edited_df['Item'].iloc[0]}")
                st.write(f"First item using values[0]: {edited_df['Item'].values[0]}")

                # Update Salsify button
                if st.button("Update in app Salsify view", type="primary"):
                    try:
                        st.write("1. Starting Update Process...")
                        item_number = edited_df['Item'].iloc[0]
                        st.write(f"Selected item_number: {item_number}")

                        # Debug - Show the actual data being processed
                        st.write("Debug - Data being processed:")
                        st.write(edited_df[edited_df['Item'] == item_number])

                        # Check if item exists
                        existing_item = con.execute("""
                            SELECT COUNT(*) as count 
                            FROM pig_data 
                            WHERE Item = ?
                        """, [item_number]).fetchone()[0]

                        st.write(f"2. Item {item_number} exists: {existing_item > 0}")

                        # First, delete the existing record if it exists
                        if existing_item > 0:
                            con.execute("DELETE FROM pig_data WHERE Item = ?", [item_number])

                        # Insert the edited record
                        st.write("3. Inserting edited record...")
                        edited_df['Category'] = category
                        edited_df['Status'] = status
                        edited_df = edited_df[["Item","Category","About","Status","Bullet Copy","Heading"
                            ,"Spanish Bullet Copy", "Subheading","Enhanced Product Name","Bullet Copy 1","Bullet Copy 2","Bullet Copy 3"
                            ,"Bullet Copy 4","Bullet Copy 5","Bullet Copy 6","Bullet Copy 7","Bullet Copy 8"
                            ,"Bullet Copy 9","Bullet Copy 10","Feature/Benefit 1","Feature/Benefit 2"
                            ,"FeatureBenefit 3","Feature/Benefit 4","FeatureBenefit 5","Feature/Benefit 6"
                            ,"Feature/Benefit 7","Feature/Benefit 8","Feature/Benefit 9","Feature/Benefit 10"
                            ,"Keywords","Long Description","Product ID","Product Title","SEO Enhanced Bullets 1"
                            ,"SEO Enhanced Bullets 2","SEO Enhanced Bullets 3","SEO Enhanced Bullets 4"
                            ,"SEO Enhanced Bullets 5","SEO Enhanced Bullets 6","SEO Enhanced Bullets 7"
                            ,"SEO Enhanced Bullets 8","SEO Enhanced Bullets 9","SEO Enhanced Bullets 10"
                            ,"Short Description","USP","Brand"]]
                        edited_df = edited_df.replace(['not in pig'],[''])


                        # Debug - Show data before insertion
                        st.write("Debug - Data before insertion:")
                        st.write(edited_df)

                        # Insert with explicit column selection
                        con.execute("""
                            CREATE OR REPLACE TABLE pig_data2 AS
                            SELECT * FROM (
                                SELECT * FROM pig_data WHERE Item != ? and "Item" not in  ('no item' ,'no_item')
                                UNION ALL
                                SELECT * FROM edited_df WHERE Item = ? and "Item" not in  ('no item' ,'no_item')
                            ) ordered
                            ORDER BY Item
                        """, [item_number, item_number])

                        st.write("Debug - Checking pig_data2 after insertion:")
                        check_df = con.execute("SELECT * FROM pig_data2 WHERE Item = ?", [item_number]).df()
                        st.write(check_df)

                        con.execute("""
                            CREATE OR REPLACE TABLE pig_data AS
                            SELECT DISTINCT * FROM pig_data2
                        """)

                        st.write("4. Writing to parquet...")
                        # Create local directory for parquet files
                        os.makedirs("local_data", exist_ok=True)

                        # Write to parquet
                        con.execute(f"""
                            COPY (SELECT DISTINCT 
                            
                            
                            "Item","Category","About","Status","Bullet Copy","Heading","Spanish Bullet Copy",
                            "Subheading","Enhanced Product Name","Bullet Copy 1","Bullet Copy 2","Bullet Copy 3"
                            ,"Bullet Copy 4","Bullet Copy 5","Bullet Copy 6","Bullet Copy 7","Bullet Copy 8"
                            ,"Bullet Copy 9","Bullet Copy 10","Feature/Benefit 1","Feature/Benefit 2"
                            ,"FeatureBenefit 3","Feature/Benefit 4","FeatureBenefit 5","Feature/Benefit 6"
                            ,"Feature/Benefit 7","Feature/Benefit 8","Feature/Benefit 9","Feature/Benefit 10"
                            ,"Keywords","Long Description","Product ID","Product Title","SEO Enhanced Bullets 1"
                            ,"SEO Enhanced Bullets 2","SEO Enhanced Bullets 3","SEO Enhanced Bullets 4"
                            ,"SEO Enhanced Bullets 5","SEO Enhanced Bullets 6","SEO Enhanced Bullets 7"
                            ,"SEO Enhanced Bullets 8","SEO Enhanced Bullets 9","SEO Enhanced Bullets 10"
                            ,"Short Description","USP","Brand"
                             
                             
                             
                             
                             
                             
                             FROM pig_data WHERE Status = ? ) 
                            TO 'local_data/pig-info-table/Status={status}/data_0.parquet' 
                            (FORMAT PARQUET, OVERWRITE 1)
                        """, [status])

                        st.write("5. Uploading to Azure...")
                        # Get connection string
                        config = ConfigParser()
                        app_path = os.path.realpath(os.path.join(os.path.abspath(''), ".."))
                        config_file = os.path.join(app_path, "resources", 'config.ini')
                        config.read(config_file)

                        # Upload the file
                        blob_service_client = BlobServiceClient(
                            account_url=AZURE_ACCOUNT_URL,
                            credential=st.session_state.sas_token
                        )
                        container_client = blob_service_client.get_container_client(st.secrets["AZ_CONTAINER"])
                        blob_path = f"app-data/pig-info-table.parquet/Status={status}/data_0.parquet"
                        
                        with open(f"local_data/pig-info-table/Status={status}/data_0.parquet", "rb") as data:
                            container_client.upload_blob(
                                name=blob_path,
                                data=data,
                                overwrite=True
                            )

                        st.success("✅ Successfully updated Salsify and uploaded to Azure!")

                        # Refresh the display data
                        st.session_state.df = con.execute("SELECT DISTINCT * FROM pig_data").df()
                        st.session_state.filtered_df = st.session_state.df

                        try:
                            blob_service_client = BlobServiceClient(
                                account_url=AZURE_ACCOUNT_URL,
                                credential=st.session_state.sas_token
                            )
                            shared_container_client = blob_service_client.get_container_client(st.secrets["AZ_CONTAINER"])
                        
                            # Get the item number for the filename
                            item_number = edited_df['Item'].iloc[0]
                            
                            # Set filename using new format
                            blob_name = f"pig-repository/{item_number}_PIG.xlsx"  # New naming format
                        
                            # Save edited data to Excel in memory
                            excel_buffer = io.BytesIO()
                            file_to_process.seek(0)  # Reset file pointer
                            excel_buffer.write(file_to_process.read())
                            excel_buffer.seek(0)
                        
                            # Upload to blob storage
                            blob_client = shared_container_client.get_blob_client(blob_name)
                            blob_client.upload_blob(excel_buffer.getvalue(), overwrite=True)
                        
                            st.success(f"✅ Successfully saved {blob_name} to PIG Repository!")
                        except Exception as e:
                            st.error(f"Error saving to PIG Repository: {str(e)}")
                
                        st.rerun()
                
                    except Exception as e:
                        st.error(f"Error updating Salsify data: {str(e)}")
                        st.error(f"Detailed error: {type(e).__name__}")
                        st.error("Full traceback:")

        except Exception as e:
            st.error(f"Error processing uploaded file: {str(e)}")

def setup_local_directories():
    """Create local directories for data storage"""
    # Create base directory for local data
    base_dir = os.path.join(os.getcwd(), 'local_data')

    # Create subdirectories
    pig_data_dir = os.path.join(base_dir, 'pig-info-table')
    reference_dir = os.path.join(base_dir, 'reference')

    # Create all directories
    os.makedirs(pig_data_dir, exist_ok=True)
    os.makedirs(reference_dir, exist_ok=True)

    return base_dir, pig_data_dir, reference_dir


def load_essential_data(sas_token):
    """Load initial essential data (Active status and reference data)"""
    base_dir, pig_data_dir, reference_dir = setup_local_directories()

    # Create progress placeholder
    progress_placeholder = st.empty()
    progress_placeholder.text("Loading essential data...")

    # Download active status data
    success, active_path = download_from_blob(
        sas_token,  # Use sas_token instead of connection_string
        st.secrets["AZ_CONTAINER"],
        "salsify-product-info/app-data/pig-info-table.parquet/Status=active/data_0.parquet",
        pig_data_dir,
        "data_0.parquet"
    )
    if not success:
        return False, None, None

    # Download reference data
    reference_files = {
        'category': 'salsify-product-info/app-data/validation/category_values.csv',
        'status': 'salsify-product-info/app-data/validation/status_values.csv'
    }

    for ref_type, blob_path in reference_files.items():
        success, _ = download_from_blob(
            sas_token,  # Changed to sas_token
            st.secrets["AZ_CONTAINER"],
            blob_path,
            reference_dir,
            f"{ref_type}_values.csv"
        )
        if not success:
            return False, None, None

    progress_placeholder.text("")

    # Load into DuckDB
    con = duckdb.connect(database=':memory:')

    # Load active data
    con.execute(f"""
    CREATE OR REPLACE TABLE pig_data AS 
    SELECT distinct * FROM read_parquet('{os.path.join(pig_data_dir,"Status=active", "data_0.parquet")}')

    """)

    # Load reference data
    con.execute(f"""
    CREATE OR REPLACE TABLE category_values AS 
    SELECT * FROM read_csv('{os.path.join(reference_dir, "category_values.csv")}', header=true) order by 2
    """)

    con.execute(f"""
    CREATE OR REPLACE TABLE status_values AS 
    SELECT * FROM read_csv('{os.path.join(reference_dir, "status_values.csv")}', header=true)  order by 1
    """)

    return True, con, (pig_data_dir, reference_dir)



def load_additional_data(sas_token, con, pig_data_dir):  # Change parameter name
    try:
        additional_statuses = {
            'New': 'app-data/pig-info-table.parquet/Status=New/data_0.parquet',
            'Obsolete': 'app-data/pig-info-table.parquet/Status=Obsolete/data_0.parquet'
        }

        for status, blob_path in additional_statuses.items():
            success, path = download_from_blob(
                sas_token,  # Use sas_token instead of connection_string
                st.secrets["AZ_CONTAINER"],
                blob_path,
                pig_data_dir,
                f"Status={status}/data_0.parquet"
            )
            if success:
                # Append to existing table using DISTINCT to avoid duplicates
                con.execute(f"""
                INSERT INTO pig_data 
                SELECT DISTINCT * FROM read_parquet('{os.path.join(pig_data_dir, "*", "data_0.parquet")}')
                WHERE Status = ?
                """, [status])

        return True
    except Exception as e:
        st.error(f"Error loading additional data: {str(e)}")
        return False

def show_salsify_upload(con):
    """Show Salsify upload interface"""
    st.header("Upload To Salsify")

    # Add description
    st.markdown("""
    This tool will:
    1. Create a backup of the existing hbb_salsify file
    2. Download the vendor's salsify.xlsx file for columns AT-BO
    3. Combine with your session data for columns A-AS
    4. Generate a new combined Excel file
    5. Upload the new file as hbb_salsify.xlsx to Salsify SFTP
    """)

    # Show preview if available
    if hasattr(st.session_state, 'preview_df'):
        st.subheader("File Preview")

        # Use the filtered data from session state if available
        display_df = st.session_state.get('filtered_preview_df', st.session_state.preview_df)
        st.dataframe(display_df)

        if st.button("Clear Preview"):
            del st.session_state.preview_df
            if 'filtered_preview_df' in st.session_state:
                del st.session_state.filtered_preview_df
            if 'preview_filename' in st.session_state:
                del st.session_state.preview_filename
            st.rerun()

    # Add upload functionality
    st.subheader("Upload to Salsify")
    col1, col2 = st.columns([1, 3])
    with col1:
        if st.button("Upload to Salsify", type="primary"):
            upload_to_salsify(con, st.session_state.sas_token)

def upload_to_salsify(con, sas_token):
    """
    Read parquet data, create Excel file that combines both the vendor-managed 
    and app-managed data, then upload to Azure blob storage and Salsify SFTP
    with history backup
    """
    try:
        # Create progress container
        progress_container = st.empty()
        progress_container.info("Starting Salsify upload process...")

        # Step 1: Read session data (our managed data, columns A-AS)
        progress_container.info("Reading session data...")
        session_df = con.execute("""
            SELECT DISTINCT * EXCLUDE(Status) FROM pig_data 
            ORDER BY Item
        """).df()

        # Step 2: Download vendor's salsify.xlsx
        progress_container.info("Downloading vendor's Salsify data...")
        try:
            # SFTP Connection details
            HOSTNAME = st.secrets["SALSIFY_HOSTNAME"]
            USERNAME = st.secrets["SALSIFY_USERNAME"]
            PASSWORD = st.secrets["SALSIFY_PASSWORD"]
            
            # Local path to save the file temporarily
            temp_file_path = os.path.join(os.getcwd(), "temp_salsify_download.xlsx")
            
            # Connect to FTP server
            ftp_server = ftplib.FTP(HOSTNAME, USERNAME, PASSWORD)
            
            # Download the file
            with open(temp_file_path, "wb") as file:
                ftp_server.retrbinary(f"RETR salsify.xlsx", file.write)
            
            # Close FTP connection
            ftp_server.quit()
            
            # Read the vendor file (all columns)
            vendor_df = pd.read_excel(temp_file_path)
            
            # Identify 'Item' column and columns AT-BO
            # For Item, assume it's in column A
            item_col = vendor_df.columns[0]
            
            # For AT-BO, convert Excel column names to indices
            # AT is 46th column (0-indexed) if A is column 0
            # BO is 67th column (0-indexed) if A is column 0
            at_idx = 45
            bo_idx = 66
            
            # Check if vendor_df has enough columns
            if vendor_df.shape[1] > bo_idx:
                # Extract the columns we need
                at_bo_cols = vendor_df.columns[at_idx:bo_idx+1]
                vendor_data = vendor_df[[item_col] + list(at_bo_cols)]
                
                # Rename Item column to match session data
                vendor_data = vendor_data.rename(columns={item_col: 'Item'})
            else:
                progress_container.warning(f"Vendor file doesn't contain expected columns AT-BO. Using session data only.")
                vendor_data = None
                
            # Clean up temporary file
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
                
        except Exception as e:
            progress_container.warning(f"Could not download or process vendor's Salsify data: {str(e)}. Continuing with session data only.")
            vendor_data = None
        
        # Step 3: Merge data based on Item ID
        progress_container.info("Merging data...")
        
        if vendor_data is not None and not vendor_data.empty:
            # Get all unique items
            all_items = pd.Series(pd.concat([session_df['Item'], vendor_data['Item']]).unique())
            
            # Create a merged dataframe with all items
            merged_df = pd.DataFrame({'Item': all_items})
            
            # Left join with session data (columns A-AS)
            merged_df = pd.merge(merged_df, session_df, on='Item', how='left')
            
            # Left join with vendor data for AT-BO columns
            merged_df = pd.merge(merged_df, vendor_data, on='Item', how='left')
            
            # Fill NA values with empty strings
            merged_df = merged_df.fillna('')
        else:
            # Just use session data if vendor data is not available
            merged_df = session_df.copy()
            merged_df = merged_df.fillna('')
        
        # Step 4: Create Excel file in memory
        progress_container.info("Creating combined Excel file...")
        excel_buffer = io.BytesIO()
        merged_df.to_excel(excel_buffer, index=False, engine='openpyxl')
        excel_buffer.seek(0)
        
        # Step 5: Create backup of existing file in Azure
        progress_container.info("Creating Azure backup...")
        blob_service_client = BlobServiceClient(
            account_url=AZURE_ACCOUNT_URL,
            credential=sas_token
        )
        container_client = blob_service_client.get_container_client(st.secrets["AZ_CONTAINER"])
        
        # Generate timestamp for backup
        timestamp = datetime.now(pytz.UTC).strftime("%Y%m%d_%H%M%S")
        
        # Check if current file exists and create backup
        try:
            blob_client = container_client.get_blob_client("salsify-sftp/hbb_salsify.xlsx")
            existing_data = blob_client.download_blob()
            
            # Upload to history with timestamp
            history_blob_client = container_client.get_blob_client(
                f"salsify-sftp/history/hbb_salsify-{timestamp}.xlsx"
            )
            history_blob_client.upload_blob(existing_data.readall(), overwrite=True)
            progress_container.info("Azure backup created successfully")
        except Exception as e:
            progress_container.warning(f"No existing file found or Azure backup failed: {str(e)}")
        
        # Step 6: Upload new Excel file to Azure
        progress_container.info("Uploading new Excel file to Azure...")
        blob_client = container_client.get_blob_client("salsify-sftp/hbb_salsify.xlsx")
        blob_client.upload_blob(excel_buffer.getvalue(), overwrite=True)
        
        # Step 7: Save Excel file temporarily for SFTP upload
        progress_container.info("Preparing for SFTP upload...")
        temp_file_path = os.path.join(os.getcwd(), "temp_hbb_salsify.xlsx")
        with open(temp_file_path, "wb") as temp_file:
            excel_buffer.seek(0)
            temp_file.write(excel_buffer.getvalue())
        
        # Step 8: Upload to Salsify SFTP
        progress_container.info("Uploading to Salsify SFTP...")
        try:
            # SFTP Connection details
            HOSTNAME = st.secrets["SALSIFY_HOSTNAME"]
            USERNAME = st.secrets["SALSIFY_USERNAME"]
            PASSWORD = st.secrets["SALSIFY_PASSWORD"]
            
            # Connect to FTP server
            ftp_server = ftplib.FTP(HOSTNAME, USERNAME, PASSWORD)
            
            # Upload the file
            with open(temp_file_path, "rb") as file:
                ftp_server.storbinary(f"STOR hbb_salsify.xlsx", file)
            
            # Close FTP connection
            ftp_server.quit()
            progress_container.info("SFTP upload completed successfully")
            
        except Exception as e:
            progress_container.error(f"SFTP upload failed: {str(e)}")
            raise e
            
        finally:
            # Clean up temporary file
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
        
        # Success message
        progress_container.success("✅ Upload to both Azure and Salsify SFTP completed successfully!")
        
        # Show summary
        st.metric("Records Uploaded", len(merged_df))
        if 'Category' in merged_df.columns:
            st.metric("Categories", merged_df['Category'].nunique())
        
        return True
        
    except Exception as e:
        progress_container.error(f"Error during upload process: {str(e)}")
        return False
            
def validate_sas(sas_token):
    """Validate SAS token by attempting a directory-level blob operation"""
    try:
        # Create the blob service client
        blob_service_client = BlobServiceClient(
            account_url=AZURE_ACCOUNT_URL,
            credential=sas_token
        )
        
        # Try to list blobs in a specific directory we know exists
        # Use a hardcoded path to a directory we know should exist
        container_client = blob_service_client.get_container_client(st.secrets["AZ_CONTAINER"])
        blobs = container_client.list_blobs(name_starts_with="salsify-product-info/")
        next(blobs, None)  # Just try to get the first blob
            
        return True

    except Exception as e:
        st.error(f"SAS validation error: {str(e)}")
        return False

    except Exception as e:
        st.error(f"SAS validation error: {str(e)}")
        return False, None


def main():
    initialize_session_state()

    if not st.session_state.authenticated:
        sas_token = st.text_input("Enter your code:", type="password")
        if st.button("Enter"):
            if validate_sas(sas_token):
                st.session_state.authenticated = True
                st.session_state.sas_token = sas_token
                st.rerun()
            else:
                st.error("Invalid access key. Please check your SAS token and try again.")
        return

    # Load essential data if needed
    if 'con' not in st.session_state or st.session_state.con is None:
        if not st.session_state.data_loaded:
            with st.spinner("Loading essential data..."):
                success, con, dirs = load_essential_data(st.session_state.sas_token)
                if success:
                    st.session_state.con = con
                    st.session_state.data_loaded = True
                    st.session_state.dirs = dirs

                    # Load additional data
                    with st.spinner("Loading additional data..."):
                        load_additional_data(st.session_state.sas_token, st.session_state.con, st.session_state.dirs[0])
                        st.session_state.additional_data_loaded = True

                    # Load initial data
                    df = con.execute("SELECT DISTINCT * FROM pig_data").df()
                    st.session_state.df = df
                    st.session_state.filtered_df = df
                else:
                    st.error("Failed to load essential data")
                    return

    # Show UI
    show_sidebar(st.session_state.con)  # Pass the database connection to show_sidebar
    main_content()


if __name__ == "__main__":
    main()
