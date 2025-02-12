# blob_navigation var_003.py

from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from datetime import datetime
import streamlit as st
import pytz
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError, ClientAuthenticationError


@dataclass
class BlobItem:
    """Represents a blob item with its metadata"""
    name: str
    full_path: str
    last_modified: datetime
    size: Optional[int] = None
    content_type: Optional[str] = None


class BlobNavigatorConfig:
    """Configuration for SAS token-based blob navigation"""

    def __init__(
            self,
            account_url: str,
            sas_token: str,
            allowed_containers: Optional[List[str]] = None,
            allowed_extensions: Optional[List[str]] = None,
            show_hidden: bool = False,
            timezone: str = 'US/Eastern',
            placeholder_text: str = "Search for files...",
            show_file_details: bool = True,
            path_prefix: Optional[str] = None  # New parameter for prefix filtering
    ):
        self.account_url = account_url.rstrip('/')
        self.sas_token = sas_token.lstrip('?')
        self.allowed_containers = allowed_containers or []
        self.allowed_extensions = allowed_extensions
        self.show_hidden = show_hidden
        self.timezone = timezone
        self.placeholder_text = placeholder_text
        self.show_file_details = show_file_details
        self.path_prefix = path_prefix.rstrip('/') + '/' if path_prefix else None  # Normalize prefix format


class BlobNavigator:
    """Azure Blob Storage navigator using SAS token authentication"""

    def __init__(
            self,
            config: BlobNavigatorConfig,
            key_prefix: str = "blob_nav"
    ):
        self.config = config
        self.key_prefix = key_prefix
        self._initialize_state()

    def _initialize_state(self):
        """Initialize session state variables"""
        state_vars = {
            'current_container': None,
            'selected_file': None,
            'error_message': None,
            'file_list': None,
            'service_client': None
        }

        for key, default_value in state_vars.items():
            full_key = f"{self.key_prefix}_{key}"
            if full_key not in st.session_state:
                st.session_state[full_key] = default_value

    def _get_blob_service_client(self) -> BlobServiceClient:
        """Get or create Azure blob service client"""
        state_key = f"{self.key_prefix}_service_client"

        if st.session_state[state_key] is None:
            try:
                # Construct the full URL with SAS token
                sas_url = f"{self.config.account_url}?{self.config.sas_token}"
                st.session_state[state_key] = BlobServiceClient(account_url=sas_url)
            except Exception as e:
                self._update_state(error_message=f"Failed to create blob service client: {str(e)}")
                raise

        return st.session_state[state_key]

    def _list_blobs(self, container: str) -> List[BlobItem]:
        """List all blobs in the container that match the configuration"""
        try:
            client = self._get_blob_service_client()
            container_client = client.get_container_client(container)
            items = []

            # If prefix is specified, use it in the list_blobs call for better performance
            prefix = self.config.path_prefix if self.config.path_prefix else None
            for item in container_client.list_blobs(name_starts_with=prefix):
                if self._should_show_item(item.name):
                    items.append(BlobItem(
                        name=item.name,
                        full_path=item.name,
                        last_modified=item.last_modified,
                        size=item.size,
                        content_type=item.content_settings.content_type
                    ))

            return sorted(items, key=lambda x: x.name.lower())
        except Exception as e:
            self._update_state(error_message=f"Error listing blobs: {str(e)}")
            return []

    def _should_show_item(self, name: str) -> bool:
        """Determine if an item should be shown based on configuration"""
        # Check prefix first if it's set
        if self.config.path_prefix and not name.startswith(self.config.path_prefix):
            return False

        if not self.config.show_hidden and name.startswith('.'):
            return False

        if not self.config.allowed_extensions:
            return True

        return any(name.lower().endswith(ext.lower())
                   for ext in self.config.allowed_extensions)

    def render_navigation(self) -> Optional[str]:
        """Render the navigation interface"""
        try:
            if not self.config.allowed_containers:
                st.error("No allowed containers configured")
                return None

            # Use first allowed container if only one is specified
            container = self.config.allowed_containers[0]
            self._update_state(current_container=container)

            # Get or update file list
            if self._state['file_list'] is None:
                self._update_state(file_list=self._list_blobs(container))

            files = self._state['file_list']
            if not files:
                st.info(f"No files found in container: {container}")
                if self.config.path_prefix:
                    st.info(f"Using prefix filter: {self.config.path_prefix}")
                return None

            # Remove prefix from display names if prefix is set
            display_names = [""]
            if self.config.path_prefix:
                display_names.extend(
                    item.name[len(self.config.path_prefix):]
                    for item in files
                )
            else:
                display_names.extend(item.name for item in files)

            # Searchable dropdown for files
            selected_display_name = st.selectbox(
                "Select File",
                options=display_names,
                key=f"{self.key_prefix}_file_select",
                placeholder=self.config.placeholder_text
            )

            if selected_display_name:
                # Reconstruct full path if using prefix
                full_path = (
                    f"{self.config.path_prefix}{selected_display_name}"
                    if self.config.path_prefix
                    else selected_display_name
                )

                selected_file = next(
                    (item for item in files if item.name == full_path),
                    None
                )

                if selected_file:
                    self._update_state(selected_file=selected_file.full_path)

                    # Show file details if configured
                    if self.config.show_file_details:
                        col1, col2 = st.columns(2)
                        with col1:
                            if selected_file.size:
                                st.write(f"Size: {self._format_size(selected_file.size)}")
                        with col2:
                            local_tz = pytz.timezone(self.config.timezone)
                            local_time = selected_file.last_modified.astimezone(local_tz)
                            st.write(f"Modified: {local_time.strftime('%Y-%m-%d %I:%M %p')}")

            # Error handling
            if self._state['error_message']:
                st.error(self._state['error_message'])
                self._update_state(error_message=None)

            return self._state['selected_file']

        except Exception as e:
            st.error(f"Navigation error: {str(e)}")
            return None

    @property
    def _state(self) -> Dict[str, Any]:
        """Get current state variables"""
        return {
            key.replace(f"{self.key_prefix}_", ''): st.session_state[key]
            for key in st.session_state
            if key.startswith(self.key_prefix)
        }

    def _update_state(self, **kwargs):
        """Update session state variables"""
        for key, value in kwargs.items():
            st.session_state[f"{self.key_prefix}_{key}"] = value

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """Format file size in human-readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f} PB"

    def load_file_content(self, blob_path: str) -> Optional[bytes]:
        """Load the content of a selected file"""
        try:
            client = self._get_blob_service_client()
            container_client = client.get_container_client(self._state['current_container'])
            blob_client = container_client.get_blob_client(blob_path)
            return blob_client.download_blob().readall()
        except Exception as e:
            self._update_state(error_message=f"Error loading file content: {str(e)}")
            return None
