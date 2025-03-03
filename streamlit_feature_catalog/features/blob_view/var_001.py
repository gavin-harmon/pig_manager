"""
blob_view.py - File View Manager for Azure Blob Storage
"""
from typing import Callable, Dict, Optional
import streamlit as st
import pandas as pd
import io
import json
from PIL import Image


class BlobViewManager:
    """Manages view handlers for different file types"""

    def __init__(self):
        self.view_handlers: Dict[str, Callable] = {}
        self._register_default_handlers()

    def _register_default_handlers(self):
        """Register built-in View handlers"""
        self.register_handler('.csv', self._view_csv)
        self.register_handler('.parquet', self._view_parquet)
        self.register_handler('.json', self._view_json)
        self.register_handler('.txt', self._view_text)
        self.register_handler('.png', self._view_image)
        self.register_handler('.jpg', self._view_image)
        self.register_handler('.jpeg', self._view_image)

    def register_handler(self, file_extension: str, handler_func: Callable):
        """Register a new view handler for a file type"""
        self.view_handlers[file_extension.lower()] = handler_func

    def can_view(self, file_path: str) -> bool:
        """Check if a file type can be viewed"""
        ext = self._get_file_extension(file_path)
        return ext.lower() in self.view_handlers

    def view_file(self, file_path: str, content: bytes, max_view_size: int = 1000) -> Optional[bool]:
        """View a file using the appropriate handler"""
        try:
            ext = self._get_file_extension(file_path)
            if ext.lower() in self.view_handlers:
                st.write("### File View")
                return self.view_handlers[ext.lower()](content, max_view_size)
            return None
        except Exception as e:
            st.error(f"Error viewing file: {str(e)}")
            return None

    @staticmethod
    def _get_file_extension(file_path: str) -> str:
        """Get file extension from path"""
        import os
        return os.path.splitext(file_path)[1].lower()

    @staticmethod
    def _view_csv(content: bytes, max_rows: int = 1000):
        """View CSV files"""
        df = pd.read_csv(io.BytesIO(content), nrows=max_rows)
        st.dataframe(df)
        return True

    @staticmethod
    def _view_parquet(content: bytes, max_rows: int = 1000):
        """View Parquet files"""
        df = pd.read_parquet(io.BytesIO(content))
        st.dataframe(df.head(max_rows))
        return True

    @staticmethod
    def _view_json(content: bytes, max_size: int = 1000):
        """View JSON files"""
        try:
            data = json.loads(content)
            st.json(data)
            return True
        except json.JSONDecodeError:
            st.error("Invalid JSON file")
            return False

    @staticmethod
    def _view_text(content: bytes, max_size: int = 1000):
        """View text files"""
        text = content.decode('utf-8', errors='ignore')
        if len(text) > max_size:
            text = text[:max_size] + "..."
        st.text(text)
        return True

    @staticmethod
    def _view_image(content: bytes, max_size: int = None):
        """View image files"""
        try:
            image = Image.open(io.BytesIO(content))
            st.image(image)
            return True
        except Exception as e:
            st.error(f"Error loading image: {str(e)}")
            return False
