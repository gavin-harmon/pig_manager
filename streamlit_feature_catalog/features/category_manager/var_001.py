# In streamlit_feature_catalog/features/category_manager/var_001.py
## had to edit this because I did something dumb


from dataclasses import dataclass
from typing import List, Optional, Dict, Callable
import streamlit as st
import pandas as pd
from azure.storage.blob import BlobServiceClient
import io
from datetime import datetime
import logging
from pathlib import Path


@dataclass
class CategoryManagerConfig:
    """Configuration for CategoryManager"""
    source_path: str  # Full path to source file
    file_type: str = "auto"  # "auto", "csv", "parquet", or "xlsx"
    require_unique_rows: bool = True
    track_changes: bool = True

    def __post_init__(self):
        if self.file_type == "auto":
            ext = Path(self.source_path).suffix.lower().lstrip('.')
            self.file_type = ext if ext in ['parquet', 'csv', 'xlsx'] else 'csv'


class CategoryManager:
    """Generic manager for categorical values with inline editing"""

    def __init__(self, config: CategoryManagerConfig, sas_token: Optional[str] = None):
        self.config = config
        self.sas_token = sas_token
        self._df = None
        self._metadata_columns = ['created_at', 'created_by', 'updated_at']
        self._initialize_session_state()
        self.load_values()

    def _initialize_session_state(self):
        """Initialize session state variables"""
        if 'last_edit_time' not in st.session_state:
            st.session_state.last_edit_time = datetime.now()
        if 'selected_column' not in st.session_state:
            st.session_state.selected_column = None
        if 'pending_changes' not in st.session_state:
            st.session_state.pending_changes = False

    def _read_data(self, content: bytes) -> pd.DataFrame:
        """Read data based on file type with proper error handling"""
        try:
            if self.config.file_type == 'parquet':
                df = pd.read_parquet(io.BytesIO(content))
            elif self.config.file_type == 'xlsx':
                df = pd.read_excel(io.BytesIO(content))
            else:
                df = pd.read_csv(io.BytesIO(content))

            # Drop duplicates based on non-metadata columns
            data_columns = [col for col in df.columns if col not in self._metadata_columns]
            df = df.drop_duplicates(subset=data_columns, keep='first')

            # Initialize tracking columns if needed
            if self.config.track_changes:
                current_time = datetime.now()
                df['created_at'] = df.get('created_at', current_time)
                df['updated_at'] = df.get('updated_at', current_time)
                df['created_by'] = df.get('created_by', st.session_state.get('user', 'unknown'))

            return df.copy()

        except Exception as e:
            logging.error(f"Error reading data: {str(e)}")
            st.error(f"Failed to read data: {str(e)}")
            return pd.DataFrame()

    def _write_data(self, df: pd.DataFrame) -> bytes:
        """Write data to bytes with proper error handling"""
        try:
            buffer = io.BytesIO()
            df_copy = df.copy()

            # Clean data before writing
            for col in df_copy.columns:
                if df_copy[col].dtype == 'object':
                    df_copy[col] = df_copy[col].apply(
                        lambda x: x.strip() if isinstance(x, str) else x
                    )

            if self.config.file_type == 'parquet':
                df_copy.to_parquet(buffer)
            elif self.config.file_type == 'xlsx':
                df_copy.to_excel(buffer, index=False, engine='openpyxl')
            else:
                df_copy.to_csv(buffer, index=False)

            buffer.seek(0)
            return buffer.getvalue()

        except Exception as e:
            logging.error(f"Error writing data: {str(e)}")
            raise RuntimeError(f"Failed to write data: {str(e)}")

    def load_values(self) -> bool:
        """Load values from source with proper error handling"""
        try:
            if self.sas_token:
                container_name, blob_path = self.config.source_path.split('/', 1)
                blob_service_client = BlobServiceClient(
                    account_url="https://daorgshare.blob.core.windows.net",
                    credential=self.sas_token
                )
                container_client = blob_service_client.get_container_client(container_name)
                blob_client = container_client.get_blob_client(blob_path)
                content = blob_client.download_blob().readall()
            else:
                import requests
                response = requests.get(self.config.source_path)
                response.raise_for_status()
                content = response.content
    
            self._df = self._read_data(content)
            return bool(len(self._df))
    
        except Exception as e:
            logging.error(f"Error loading values: {str(e)}")
            st.error(f"Failed to load values: {str(e)}")
            return False

    def save_values(self) -> bool:
        """Save values to source with proper validation and error handling"""
        try:
            if self._df is None or len(self._df) == 0:
                raise ValueError("No data to save")
    
            # Update metadata
            if self.config.track_changes:
                self._df['updated_at'] = datetime.now()
    
            # Save to appropriate location
            if self.sas_token:
                container_name, blob_path = self.config.source_path.split('/', 1)
                blob_service_client = BlobServiceClient(
                    account_url="https://daorgshare.blob.core.windows.net",
                    credential=self.sas_token
                )
                container_client = blob_service_client.get_container_client(container_name)
                blob_client = container_client.get_blob_client(blob_path)
    
                content = self._write_data(self._df)
                blob_client.upload_blob(content, overwrite=True)
            else:
                # Implement local file saving if needed
                with open(self.config.source_path, 'wb') as f:
                    content = self._write_data(self._df)
                    f.write(content)
    
            st.session_state.pending_changes = False
            return True
    
        except Exception as e:
            logging.error(f"Error saving values: {str(e)}")
            st.error(f"Failed to save values: {str(e)}")
            return False

    def render_manager(self):
        """Main render function with improved error handling and state management"""
        if self._df is None:
            st.error("No data loaded")
            return

        st.title("Category Manager")

        categorical_columns = sorted([
            col for col in self._df.select_dtypes(include=['object']).columns
            if col not in self._metadata_columns
        ])

        if not categorical_columns:
            st.warning("No categorical columns found in the dataset")
            return

        selected_column = st.selectbox(
            "Select Column to Manage",
            options=categorical_columns,
            key="column_selector"
        )

        if not selected_column:
            st.warning("Please select a column to manage")
            return

        st.session_state.selected_column = selected_column

        # Show value distribution
        st.subheader("Value Distribution")
        unique_values = self._df[selected_column].value_counts()
        st.bar_chart(unique_values)

        # Add new value section
        with st.expander("Add New Entry", expanded=False):
            self._render_add_form(selected_column)

        # Create and display table first (without delete button)
        st.subheader("Current Values")
        display_df, edited_df = self._render_editable_table(selected_column)

        # Add delete button here, between Add Entry and table
        if st.button("🗑️ Delete Selected"):
            try:
                selected_rows = edited_df[edited_df['Select'] == True]
                if len(selected_rows) == 0:
                    st.warning("Please select rows to delete")
                    return

                # Get indices to delete
                indices_to_delete = selected_rows.index.tolist()

                # Update the main DataFrame
                self._df = self._df.drop(indices_to_delete).reset_index(drop=True)

                # Save changes immediately
                if self.save_values():
                    st.success(f"Deleted {len(selected_rows)} entries")
                    # Clear the editor state to force a refresh
                    if f"value_editor_{selected_column}" in st.session_state:
                        del st.session_state[f"value_editor_{selected_column}"]
                    st.rerun()
                else:
                    st.error("Failed to save changes")

            except Exception as e:
                st.error(f"Error deleting rows: {str(e)}")

        # Display the table
        st.data_editor(
            display_df,
            use_container_width=True,
            hide_index=True,
            key=f"value_editor_{selected_column}",
            column_config={
                "Select": st.column_config.CheckboxColumn(
                    "Select",
                    help="Select row for deletion",
                    default=False,
                    width="small",
                ),
                **{
                    col: st.column_config.Column(
                        label=col.replace('_', ' ').title(),
                        width='medium',
                    )
                    for col in edited_df.columns if col != 'Select'
                }
            },
            on_change=self._handle_edit_change,
            disabled=False,
            num_rows="dynamic"
        )

    def _render_add_form(self, column: str):
        """Render add form with improved validation"""
        editable_columns = [col for col in self._df.columns if col not in self._metadata_columns]

        with st.form("add_value_form", clear_on_submit=True):
            new_values = {}
            cols = st.columns(2)
            for i, col in enumerate(editable_columns):
                with cols[i % 2]:
                    new_values[col] = st.text_input(f"{col.replace('_', ' ').title()}:")

            submitted = st.form_submit_button("Add Entry")
            if submitted:
                try:
                    if not new_values[column]:
                        raise ValueError(f"Primary column '{column}' cannot be empty")

                    # Filter out empty values
                    new_values = {k: v for k, v in new_values.items() if v.strip() != ""}

                    # Create new row DataFrame
                    new_row = pd.DataFrame([new_values])

                    # Add to main DataFrame
                    self._df = pd.concat([self._df, new_row], ignore_index=True)

                    # Save immediately
                    if self.save_values():
                        st.success("New entry added!")
                        # Clear the editor state to force a refresh with new data
                        if f"value_editor_{column}" in st.session_state:
                            del st.session_state[f"value_editor_{column}"]
                        st.rerun()
                    else:
                        st.error("Failed to save new entry")

                except Exception as e:
                    st.error(str(e))

    def _render_editable_table(self, column: str):
        """Prepare data for editable table"""
        editable_columns = [col for col in self._df.columns if col not in self._metadata_columns]
        display_df = self._df[editable_columns].copy()
        display_df = display_df.astype(str).replace('nan', '')
        display_df['Select'] = False

        return display_df, display_df  # Return both display and edited versions

    def _handle_edit_change(self):
        """Handle edit changes with improved validation"""
        try:
            editor_key = f"value_editor_{st.session_state.selected_column}"
            if editor_key not in st.session_state:
                return

            edited_data = st.session_state[editor_key]

            # Convert to DataFrame consistently
            if isinstance(edited_data, dict):
                edited_df = pd.DataFrame([edited_data])
            else:
                edited_df = pd.DataFrame(edited_data)

            if 'Select' in edited_df.columns:
                edited_df = edited_df.drop('Select', axis=1)

            # Update the main DataFrame with the edited data
            for col in edited_df.columns:
                self._df.loc[edited_df.index, col] = edited_df[col]

            st.session_state.pending_changes = True

        except Exception as e:
            st.error(f"Error handling changes: {str(e)}")
