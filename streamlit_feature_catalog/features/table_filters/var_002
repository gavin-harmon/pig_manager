"""
MODULAR STREAMLIT APPLICATION FRAMEWORK
=====================================

FEATURE: DATA FILTER - VARIANT 002
----------------------------
Pandas-based implementation with similar patterns to var_001 but without DuckDB
"""

import streamlit as st
import pandas as pd
from typing import Union, List, Optional, Dict
from io import BytesIO


# We can remove num_filters from DataFilterConfig since it's no longer needed
class DataFilterConfig:
    """Configuration class for DataFilter"""
    def __init__(
            self,
            allowed_extensions: List[str] = ['.csv', '.parquet', '.xlsx'],
            show_metrics: bool = True
    ):
        self.allowed_extensions = allowed_extensions
        self.show_metrics = show_metrics


class DataFilter:
    def __init__(
            self,
            data: Union[pd.DataFrame, str, bytes],
            config: DataFilterConfig = None,
            session_key: str = "data_filter"
    ):
        self.config = config or DataFilterConfig()
        self.session_key = session_key
        self._load_data(data)

        # Initialize the active filters in session state if not present
        if f"{self.session_key}_active_filters" not in st.session_state:
            st.session_state[f"{self.session_key}_active_filters"] = 1

    def _load_data(self, data):
        """Load data with proper error handling"""
        try:
            if isinstance(data, pd.DataFrame):
                self.df = data.copy()
            elif isinstance(data, bytes):
                try:
                    self.df = pd.read_parquet(BytesIO(data))
                except:
                    self.df = pd.read_csv(BytesIO(data))
            elif isinstance(data, str):
                if data.endswith('.parquet'):
                    self.df = pd.read_parquet(data)
                elif data.endswith('.csv'):
                    self.df = pd.read_csv(data)
                elif data.endswith(('.xlsx', '.xls')):
                    self.df = pd.read_excel(data)
            elif hasattr(data, 'getvalue'):
                return self._load_data(data.getvalue())
            else:
                raise ValueError(f"Unsupported data format: {type(data)}")

            if self.df.empty:
                raise ValueError("No data loaded")

        except Exception as e:
            st.error(f"Error loading data: {str(e)}")
            raise

    def _get_column_type(self, column: str) -> str:
        """Determine column type for filtering"""
        dtype = self.df[column].dtype
        unique_count = self.df[column].nunique()

        if pd.api.types.is_string_dtype(self.df[column]):
            return 'categorical' if unique_count <= 100 else 'text'
        elif pd.api.types.is_numeric_dtype(self.df[column]):
            return 'numeric'
        elif pd.api.types.is_datetime64_any_dtype(self.df[column]):
            return 'datetime'
        return 'categorical'

    def _apply_filters_up_to(self, filter_num: int) -> pd.DataFrame:
        """Apply all filters up to a specific filter number"""
        filtered_df = self.df.copy()

        # Get current active filters from session state
        filters = []
        for i in range(filter_num):
            filter_col = st.session_state.get(f"{self.session_key}_filter_col_{i}", '')
            filter_vals = st.session_state.get(f"{self.session_key}_filter_val_{i}", [])
            if filter_col and filter_vals:
                filters.append({'column': filter_col, 'values': filter_vals})

        # Apply each filter in sequence
        for filter_info in filters:
            filtered_df = filtered_df[filtered_df[filter_info['column']].isin(filter_info['values'])]

        return filtered_df

    def _render_filter_widget(self, filter_num: int) -> Optional[Dict]:
        """Render a single filter widget in the sidebar"""
        st.sidebar.write("")  # Add spacing

        # Get the DataFrame filtered by all previous filters
        current_filtered_df = self._apply_filters_up_to(filter_num)

        # First dropdown - select the column
        filter_col = st.sidebar.selectbox(
            f"Filter {filter_num + 1}:",
            options=[''] + list(current_filtered_df.columns),
            key=f"{self.session_key}_filter_col_{filter_num}"
        )

        # If a column was chosen and this is the last filter, immediately add a new one
        if filter_col and filter_num == st.session_state[f"{self.session_key}_active_filters"] - 1:
            st.session_state[f"{self.session_key}_active_filters"] += 1

        # Only show value selection if a column was chosen
        if filter_col:
            # Get unique values from the FILTERED dataset for this column
            unique_values = sorted(current_filtered_df[filter_col].dropna().unique())

            # Show multiselect for picking values
            selected_values = st.sidebar.multiselect(
                f"Select values from {filter_col}:",
                options=unique_values,
                default=[],  # Clear default values when filter options change
                key=f"{self.session_key}_filter_val_{filter_num}"
            )

            # Return filter info if values were selected
            if selected_values:
                return {
                    'column': filter_col,
                    'type': 'categorical',
                    'values': selected_values
                }

        return None

    def render(self) -> pd.DataFrame:
        """Render filter interface and return filtered DataFrame"""
        st.sidebar.header("Data Filters")

        # Render filter widgets
        active_filters = []
        for i in range(st.session_state[f"{self.session_key}_active_filters"]):
            filter_info = self._render_filter_widget(i)
            if filter_info:
                active_filters.append(filter_info)

        # Store filter state
        st.session_state[f"{self.session_key}_filters"] = active_filters

        # Apply all filters to get final dataset
        filtered_df = self._apply_filters_up_to(len(active_filters))

        # Store filtered dataframe
        st.session_state[f"{self.session_key}_filtered_df"] = filtered_df

        # Show filter summary if configured
        if self.config.show_metrics:
            st.sidebar.markdown("---")
            st.sidebar.markdown("### Filter Summary")
            st.sidebar.markdown(f"Total Records: {len(self.df):,}")
            st.sidebar.markdown(f"Filtered Records: {len(filtered_df):,}")
            st.sidebar.markdown(f"Active Filters: {len(active_filters)}")

        return filtered_df



