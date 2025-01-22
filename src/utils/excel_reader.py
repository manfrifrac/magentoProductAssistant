
import pandas as pd
import openpyxl
from pathlib import Path
import logging
from typing import Dict, Any, Optional

class ExcelReader:
    @staticmethod
    def read_excel_with_hyperlinks(file_path: str) -> pd.DataFrame:
        """Read Excel file while preserving hyperlinks"""
        try:
            # Load workbook with openpyxl to access hyperlinks
            wb = openpyxl.load_workbook(file_path)
            sheet = wb.active
            
            # Get hyperlinks from cells
            hyperlinks = {}
            for row in sheet.iter_rows():
                for cell in row:
                    if cell.hyperlink:
                        # Store hyperlink for this cell coordinate
                        hyperlinks[cell.coordinate] = cell.hyperlink.target

            # Read Excel normally with pandas
            df = pd.read_excel(file_path)
            
            # Add hyperlinks column if we found any
            if hyperlinks:
                # Convert cell coordinates to row/col indices
                for coord, url in hyperlinks.items():
                    row = openpyxl.utils.cell.coordinate_to_tuple(coord)[0] - 1  # 0-based index
                    if 0 <= row < len(df):
                        # Add hyperlink to a new column
                        if 'hyperlink' not in df.columns:
                            df['hyperlink'] = ''
                        df.at[row, 'hyperlink'] = url

            return df

        except Exception as e:
            logging.error(f"Error reading Excel file with hyperlinks: {e}")
            # Fallback to regular pandas read
            return pd.read_excel(file_path)

    @staticmethod
    def extract_url_from_cell(cell_value: Any) -> Optional[str]:
        """Extract URL from cell value, handling various formats"""
        if pd.isna(cell_value):
            return None
            
        # Handle hyperlink formula
        if isinstance(cell_value, str):
            if '=HYPERLINK' in cell_value:
                # Extract URL from HYPERLINK formula
                import re
                match = re.search(r'=HYPERLINK\("([^"]+)"', cell_value)
                if match:
                    return match.group(1)
            elif 'http://' in cell_value or 'https://' in cell_value:
                # Direct URL in cell
                return cell_value.strip()
                
        # Handle tuple format (display_text, url)
        if isinstance(cell_value, tuple) and len(cell_value) == 2:
            return cell_value[1]
            
        return None